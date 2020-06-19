#include "pqpersistent.hh"
#include "pqserver.hh"
#include <iostream>

#include <assert.h>
#include "kvs_go_api.h"

namespace pq {

#define Prefix_P 0x80000000

static void* Complex2Simple(void* key, size_t len, size_t* len_simple) {
    char* ptr = (char*)key;
    char type = ptr[0];
    if (type != 's' && type != 'p') return NULL;

    char userid_char[9]; userid_char[8] = 0;
    memcpy(userid_char, ptr+2, 8);
    uint32_t userid_int = atoi(userid_char);
    
    if (type == 'p')
        userid_int = userid_int | Prefix_P;
    
    char* newkey = (char*)malloc(len-6);
    memcpy(newkey, &userid_int, 4);
    memcpy(newkey+4, ptr+10, len-10);
    *len_simple = len-6;
    return newkey;
}

static void* Simple2Complex(void* key, size_t len, size_t* len_complex) {
    size_t newkey_len = len + 6;
    char* newkey = (char*)malloc(newkey_len);
    char* ptr = (char*)key;
    uint32_t target = *(uint32_t*)key;
    
    if (target & Prefix_P)
        sprintf(newkey, "p|%08d", target & 0x7FFFFFFF);
    else 
        sprintf(newkey, "s|%08d", target & 0x7FFFFFFF); 
       
    memcpy(newkey+10, ptr+4, len-4);
    *len_complex = newkey_len;
    return newkey;
}

KVSDBStore::KVSDBStore() {
    Kvsdb_create(&kvsdb, (char*)"KVSDBStore", 10);
    std::cout << "[DB] KVSDBStore Construction\n"; // DB log
}

KVSDBStore::~KVSDBStore() {
    Kvsdb_close(kvsdb);
    std::cout << "[DB] KVSDBStore Destruction\n"; // DB log
}

tamed void KVSDBStore::put(Str key, Str value, tamer::event<> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
        char*  val_ptr = (char*)value.data();
        size_t val_len = value.length();
    }
    assert(key_ptr);
    
    twait { Kvsdb_put(kvsdb, key_ptr, key_len, val_ptr, val_len); }
        
    std::cout << "[DB] PUT ";   // DB log
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
    fflush(stdout); 

    done();

    free(key_ptr);
}

tamed void KVSDBStore::erase(Str key, tamer::event<> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
    }
    
    twait { Kvsdb_del(kvsdb, key_ptr, key_len); }

    std::cout << "[DB] ERASE "; // DB log
    key.PrintHex(); std::cout << " " << key.length() << '\n';
    fflush(stdout);

    done();
    
    free(key_ptr);
}

tamed void KVSDBStore::get(Str key, tamer::event<String> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
        char*  val_ptr = NULL;
        size_t val_len = 0;
    }
    
    twait { val_ptr = (char*)Kvsdb_get(kvsdb, key_ptr, (int)key_len, (int*)&val_len); }

    std::cout << "[DB] GET "; // DB log
    key.PrintHex(); std::cout << " " << key.length() << '\n';
    fflush(stdout);

    if (val_ptr)
        done(String(val_ptr, val_len));
    else
        done("");
        
    free(key_ptr);
}

tamed void KVSDBStore::scan(Str first, Str last, tamer::event<ResultSet> done) {
    tvars {
        Iter kvsiter;
        std::vector<kvdata_entry*>* result;
        size_t key_first_len = 0;
        char*  key_first_ptr = (char*)Complex2Simple((char*)first.data(), first.length(), &key_first_len);
        size_t key_last_len  = 0;
        char*  key_last_ptr  = (char*)Complex2Simple((char*)last.data(), last.length(), &key_last_len);
    }

    std::cout << "[DB] SCAN "; // DB log
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << "\n";
    fflush(stdout);

    twait { 
        Kvsiter_create_with_range(kvsdb, &kvsiter, key_first_ptr, 0, key_first_ptr, key_first_len, key_last_ptr, key_last_len);    
        result = (std::vector<kvdata_entry*>*)Kvsiter_readkey(kvsiter);
    }
    
    ResultSet& rs = done.result();
    char*  key_original = NULL;
    size_t key_original_len = 0;

    for (std::vector<kvdata_entry*>::iterator iter = result->begin(); iter != result->end(); iter++) {
		kvdata_entry* e = *iter;
        key_original = (char*)Simple2Complex((char*)e->ptr_key,e->klen, &key_original_len);
        
        rs.push_back(Result(String(key_original, key_original_len), String((char*)e->ptr_val, e->vlen)));
        free(key_original); key_original = NULL;
	}
        
    done.unblocker().trigger();

    free(key_first_ptr);
    free(key_last_ptr);
    Kvsiter_free(kvsiter);
}

void KVSDBStore::flush() {
    return;
}

void KVSDBStore::run_monitor(Server& server) {
    return;
}

#if HAVE_LIBLEVELDB

LevelDBStore::LevelDBStore(const DBPoolParams& params)
    : params_(params), pool_(nullptr) {
}

#endif 

#if HAVE_LIBPQ

PostgresStore::PostgresStore(const DBPoolParams& params)
    : params_(params), pool_(nullptr), monitor_(nullptr) {
    std::cout << "[DB] PostgresStore Construction " << "\n"; // DB log
}

PostgresStore::~PostgresStore() {
    delete pool_;
    if (monitor_)
        PQfinish(monitor_);
    std::cout << "[DB] PostgresStore Destruction\n"; // DB log
}

void PostgresStore::connect() {
    std::vector<String> statements({
        "PREPARE kv_put(text,text) AS "
            "WITH upsert AS "
            "(UPDATE cache SET value=$2 WHERE key=$1 RETURNING cache.*) "
            "INSERT INTO cache "
            "SELECT * FROM (SELECT $1 k, $2 v) AS tmp_table "
            "WHERE CAST(tmp_table.k AS TEXT) NOT IN (SELECT key FROM upsert)",
        "PREPARE kv_erase(text) AS "
            "DELETE FROM cache WHERE key=$1",
        "PREPARE kv_get(text) AS "
            "SELECT value FROM cache WHERE key=$1",
        "PREPARE kv_scan(text,text) AS "
            "SELECT key, value FROM cache WHERE key >= $1 AND key < $2"});

    pool_ = new DBPool(params_);
    pool_->connect_all(statements);
    std::cout << "[DB] PostgresStore Connect\n"; // DB log
}

tamed void PostgresStore::put(Str key, Str value, tamer::event<> done) {
    tvars {
        String q = "EXECUTE kv_put('" + key + "','" + value + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    std::cout << "[DB] PUT "; // DB log
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
    
    done();
}

tamed void PostgresStore::erase(Str key, tamer::event<> done) {
    tvars {
        String q = "EXECUTE kv_erase('" + key + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    std::cout << "[DB] ERASE "; // DB log
    key.PrintHex(); std::cout << " " << key.length() << '\n';
    
    done();
}

tamed void PostgresStore::get(Str key, tamer::event<String> done) {
    tvars {
        String q = "EXECUTE kv_get('" + key + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    std::cout << "[DB] GET "; // DB log
    key.PrintHex(); std::cout << " " << key.length() << '\n';

    if (j.is_a() && j.size() && j[0].size())
        done(j[0][0].as_s());
    else
        // todo: distinguish between no value and empty value!
        done("");
}

tamed void PostgresStore::scan(Str first, Str last, tamer::event<ResultSet> done) {
    tvars {
        String q = "EXECUTE kv_scan('" + first + "','" + last + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    std::cout << "[DB] SCAN "; // DB log
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << "\n";

    ResultSet& rs = done.result();
    for (auto it = j.abegin(); it < j.aend(); ++it )
        rs.push_back(Result((*it)[0].as_s(), (*it)[1].as_s()));

    done.unblocker().trigger();
}

void PostgresStore::flush() {
    pool_->flush();
    std::cout << "[DB] FLUSH " << "\n"; // DB log
}

void PostgresStore::run_monitor(Server& server) {
    String cs = "dbname=" + params_.dbname + " host=" + params_.host + " port=" + String(params_.port);
    monitor_ = PQconnectdb(cs.c_str());
    mandatory_assert(monitor_);
    mandatory_assert(PQstatus(monitor_) != CONNECTION_BAD);

    PGresult* res;
    res = PQexec(monitor_,
                 "CREATE OR REPLACE FUNCTION notify_upsert_listener() "
                 "RETURNS trigger AS "
                 "$BODY$ "
                 "BEGIN "
                 "PERFORM pg_notify('backend_queue', '{ \"op\":0, \"k\":\"' || CAST (NEW.key AS TEXT) || '\", \"v\":\"' || CAST (NEW.value AS TEXT) || '\" }'); "
                 "RETURN NULL; "
                 "END; "
                 "$BODY$ "
                 "LANGUAGE plpgsql VOLATILE "
                 "COST 100");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE OR REPLACE FUNCTION notify_delete_listener() "
                 "RETURNS trigger AS "
                 "$BODY$ "
                 "BEGIN "
                 "PERFORM pg_notify('backend_queue', '{ \"op\":1, \"k\":\"' || CAST (OLD.key AS TEXT) || '\" }'); "
                 "RETURN NULL; "
                 "END; "
                 "$BODY$ "
                 "LANGUAGE plpgsql VOLATILE "
                 "COST 100");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "DROP TRIGGER IF EXISTS notify_upsert_cache ON cache");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "DROP TRIGGER IF EXISTS notify_delete_cache ON cache");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE TRIGGER notify_upsert_cache "
                 "AFTER INSERT OR UPDATE ON cache "
                 "FOR EACH ROW "
                 "EXECUTE PROCEDURE notify_upsert_listener()");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE TRIGGER notify_delete_cache "
                 "AFTER DELETE ON cache "
                 "FOR EACH ROW "
                 "EXECUTE PROCEDURE notify_delete_listener()");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "LISTEN backend_queue");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    monitor_db(server);
}

tamed void PostgresStore::monitor_db(Server& server) {
    tvars {
        int32_t err;
        Json j;
    }

    while(true) {
        // XXX: we might need to yield once in a while...

        do {
            twait { tamer::at_fd_read(PQsocket(monitor_), make_event()); }
            err = PQconsumeInput(monitor_);
            mandatory_assert(err == 1 && "Error reading data from DB.");
        } while(PQisBusy(monitor_));

        // there should be no results on this connection
        mandatory_assert(!PQgetResult(monitor_));

        // process notifications
        PGnotify* n = PQnotifies(monitor_);
        while(n) {

            j.assign_parse(n->extra);
            assert(j && j.is_o());

            switch(j["op"].as_i()) {
                case pg_update:
                    server.insert(j["k"].as_s(), j["v"].as_s());
                    break;

                case pg_delete:
                    server.erase(j["k"].as_s());
                    break;

                default:
                    mandatory_assert(false && "Unknown DB operation.");
                    break;
            }

            PQfreemem(n);
            n = PQnotifies(monitor_);
        }
    }
}

#endif
}
