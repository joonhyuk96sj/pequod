#include "pqpersistent.hh"
#include "pqserver.hh"
#include <iostream>
#include <assert.h>

#if HAVE_LIBKVSDB
#include "kvs_go_api.h"
#include <vector>
#include <string>
#endif

#if HAVE_LIBLEVELDB
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/cache.h"
#endif

#if HAVE_LIBROCKSDB
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#endif

namespace pq {

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

#if HAVE_LIBKVSDB

int PQ_Classify(void* key, size_t len) {
#if AGGREGATION_ON == 1
    uint32_t target = *(uint32_t*)key;
    int userid_int  = target & 0x7FFFFFFF;      
    
    if (target & Prefix_P)
        return userid_int;
    else
        return userid_int+NUM_USERS;
#else
    return -1;
#endif
}

KVSDBStore::KVSDBStore() {
    Kvsdb_create(&kvsdb, (char*)"KVSDBStore", 10);
    handler = AG_Init(kvsdb, PQ_Classify);

#if AGGREGATION_ON == 1
    for (int i=0; i<NUM_USERS; i++) {
        int num = AG_Create(handler, i, 4, 11, 2);
        printf("AG_Create : P, %d th AG with id %d\n", num, i);        
    }
    for (int i=0; i<NUM_USERS; i++) {
        int num = AG_Create(handler, i+NUM_USERS, 4, 9, 2);
        printf("AG_Create : S, %d th AG with id %d\n", num, i);        
    } 
#endif

    std::cout << "[DB] KVSDBStore Construction\n";
}

KVSDBStore::~KVSDBStore() {
    Kvsdb_close(kvsdb);
    std::cout << "[DB] KVSDBStore Destruction\n";
}

tamed void KVSDBStore::put(Str key, Str value, tamer::event<> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
        char*  val_ptr = (char*)value.data();
        size_t val_len = value.length();
    }
    assert(key_ptr);
    
    twait { AG_Put(handler, key_ptr, key_len, val_ptr, val_len); }
        
#if PrintLog == 1
    std::cout << "[DB] PUT ";
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
    //fflush(stdout);
#endif
    done();
    free(key_ptr);
}

// @ Not called
tamed void KVSDBStore::erase(Str key, tamer::event<> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
    }
    twait { Kvsdb_del(kvsdb, key_ptr, key_len); }
    done();
    free(key_ptr);
}
// @ Not called
tamed void KVSDBStore::get(Str key, tamer::event<String> done) {
    tvars {
        size_t key_len = 0;
        char*  key_ptr = (char*)Complex2Simple((char*)key.data(), key.length(), &key_len);
        char*  val_ptr = NULL;
        size_t val_len = 0;
    }
    
    twait { val_ptr = (char*)AG_Get(handler, key_ptr, (int)key_len, (int*)&val_len); }
    if (val_ptr)
        done(String(val_ptr, val_len));
    else
        done("");
    free(key_ptr);
}

tamed void KVSDBStore::scan(Str first, Str last, tamer::event<ResultSet> done) {
    tvars {
        char*  key_first_ptr = (char*)Complex2Simple((char*)first.data(), first.length(), &key_first_len);
        char*  key_last_ptr  = (char*)Complex2Simple((char*)last.data(),  last.length(),  &key_last_len);
        char*  key_ori       = NULL;
        size_t key_first_len = 0;
        size_t key_last_len  = 0;
        size_t key_ori_len   = 0;
    
        ResultSet* rs = new ResultSet();        
        std::string key, key_new, val;
        std::vector<std::pair<std::string, std::string>>* result = NULL;
        int count = 0;
    }
    assert(key_first_ptr);assert(key_last_ptr);

#if PrintLog == 1
    std::cout << "[DB] SCAN ";
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << " ";
#endif

    twait { 
        result = (std::vector<std::pair<std::string, std::string>>*)\
                AG_Scan(handler, key_first_ptr, key_first_len, key_last_ptr, key_last_len, key_first_ptr, 0, 4);    
    }
    assert(result != NULL);
    
    for (auto iter = result->begin(); iter != result->end(); iter++) {
        key = iter->first;
        val = iter->second;
        key_ori = (char*)Simple2Complex((char*)key.c_str(), key.size(), &key_ori_len);
        key_new = std::string(key_ori, key_ori_len);
        free(key_ori); key_ori = NULL; key_ori_len = 0;

        rs->emplace_back(Result(key_new, val));
        count++;
    }
    free(key_first_ptr); free(key_last_ptr);
    delete result;

#if PrintLog == 1
    std::cout << count << std::endl;
    //fflush(stdout); 
#endif
    done(*rs);
}
    
void KVSDBStore::flush() {return;}
void KVSDBStore::run_monitor(Server& server) {return;}
#endif

#if HAVE_LIBLEVELDB
LevelDBStore::LevelDBStore() {
    size_t megabyte   = 1024*1024;
    size_t cache_size = (size_t)470*megabyte;
    
    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists   = true;
    options.max_open_files    = 2500; // 2MB per 1
    options.compression       = leveldb::kNoCompression;
    options.block_cache       = leveldb::NewLRUCache(cache_size);
    
    leveldb::Status status = leveldb::DB::Open(options, "/home/joonhyuk/SSD_OPTANE/leveldb-pequod", &db);   
    std::cout << "[DB] LevelDBStore Construction" << std::endl;
}

LevelDBStore::~LevelDBStore() {
    delete db;
}

tamed void LevelDBStore::put(Str key, Str value, tamer::event<> done){
    tvars {
        leveldb::Status status;
    }

#if PrintLog == 1
    std::cout << " [DB] PUT ";
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
    //fflush(stdout);
#endif

    twait { status = db->Put(leveldb::WriteOptions(), std::string(key), std::string(value)); }
    done();
}

tamed void LevelDBStore::erase(Str key, tamer::event<> done){
    tvars {
        leveldb::Status status;
    }
    twait { status = db->Delete(leveldb::WriteOptions(), std::string(key)); }
    done();
}
tamed void LevelDBStore::get(Str key, tamer::event<String> done){
    tvars {
	    std::string value;
        leveldb::Status status;
    }
    twait { status = db->Get(leveldb::ReadOptions(), std::string(key), &value); }
    done(value);
}

tamed void LevelDBStore::scan(Str first, Str last, tamer::event<ResultSet> done){
    tvars {  
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        std::string start = std::string(first);
        std::string limit = std::string(last);
        ResultSet* rs = new ResultSet();
        int count = 0;
    }
    
#if PrintLog == 1  
    std::cout <<" [DB] SCAN ";
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << " ";
#endif
    
    twait {
        for (it->Seek(start); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key >= limit) break;
            if (strncmp(key.c_str(), start.c_str(), 10) != 0) continue;
            rs->emplace_back(Result(key, it->value().ToString()));
            count++;
        }
        assert(it->status().ok());
        delete it;
    }

#if PrintLog == 1
    std::cout << count << std::endl;
    //fflush(stdout);  
#endif
    done(*rs);
}

void LevelDBStore::flush() {return;}
void LevelDBStore::run_monitor(Server& server) {return;}
#endif 

/*
#if HAVE_LIBROCKSDB

RocksDBStore::RocksDBStore() {
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.error_if_exists   = true;
    options.compression = rocksdb::CompressionType::kNoCompression;
    
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(4500*1024*1024);
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    rocksdb::Status status = rocksdb::DB::Open(options, "/home/joonhyuk/SSD_OPTANE/rocksdb-pequod", &db);
    std::cout << "[DB] RocksDBStore Construction" << std::endl;
    assert(status.ok());
}

RocksDBStore::~RocksDBStore() {
    delete db;
}

tamed void RocksDBStore::put(Str key, Str value, tamer::event<> done){
    tvars {
        rocksdb::Status status;
    }

#if PrintLog == 1
    std::cout << " [DB] PUT ";
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
#endif

    twait { status = db->Put(rocksdb::WriteOptions(), std::string(key), std::string(value)); }
    done();
}

tamed void RocksDBStore::erase(Str key, tamer::event<> done){
    tvars {
        rocksdb::Status status;
    }
    twait { status = db->Delete(rocksdb::WriteOptions(), std::string(key)); }
    done();
}
tamed void RocksDBStore::get(Str key, tamer::event<String> done){
    tvars {
	    std::string value;
        rocksdb::Status status;
    }
    twait { status = db->Get(rocksdb::ReadOptions(), std::string(key), &value); }
    done(value);
}

tamed void RocksDBStore::scan(Str first, Str last, tamer::event<ResultSet> done){
    tvars {    
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        std::string start = std::string(first);
        std::string limit = std::string(last);
        ResultSet* rs = new ResultSet();
        int count = 0;
    }

#if PrintLog == 1
    std::cout << " [DB] SCAN ";
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << " ";
#endif
    
    twait {
        for (it->Seek(start); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key >= limit) break;
            if (strncmp(key.c_str(), start.c_str(), 10) != 0) continue;
            rs->emplace_back(Result(key, it->value().ToString()));
            count++;
        }
        assert(it->status().ok());
        delete it;
    }

#if PrintLog == 1
    std::cout << count << std::endl;  
#endif
    done(*rs);
}

void RocksDBStore::flush() {return;}
void RocksDBStore::run_monitor(Server& server) {return;}
#endif 
*/

#if HAVE_LIBROCKSDB

RocksDBStore::RocksDBStore() {
    size_t megabyte   = 1024*1024;
    size_t cache_size = (size_t)470*megabyte;

    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.error_if_exists   = true;
    options.compression = rocksdb::CompressionType::kNoCompression;
    
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(cache_size);
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    rocksdb::Status status = rocksdb::DB::Open(options, "/home/joonhyuk/SSD_OPTANE/rocksdb-pequod", &db);
    std::cout << " [DB] ROCKSDB Constructor"<<std::endl;
    assert(status.ok());
}

RocksDBStore::~RocksDBStore() {
    delete db;
}

tamed void RocksDBStore::put(Str key, Str value, tamer::event<> done){
    tvars {
        rocksdb::Status status;
    }

#if PrintLog == 1
    std::cout << " [DB] PUT ";
    key.PrintHex(); std::cout << " " << key.length() << " " << value.length() << '\n';
    //fflush(stdout);
#endif

    twait { status = db->Put(rocksdb::WriteOptions(), std::string(key), std::string(value)); }
    done();
}

tamed void RocksDBStore::erase(Str key, tamer::event<> done){
    tvars {
        rocksdb::Status status;
    }
    twait { status = db->Delete(rocksdb::WriteOptions(), std::string(key)); }
    done();
}

tamed void RocksDBStore::get(Str key, tamer::event<String> done){ 
    tvars {
	    std::string value;
        rocksdb::Status status;
    }
    twait { status = db->Get(rocksdb::ReadOptions(), std::string(key), &value); }
    done(value);
}

tamed void RocksDBStore::scan(Str first, Str last, tamer::event<ResultSet> done){
    tvars {    
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        std::string start = std::string(first);
        std::string limit = std::string(last);
        ResultSet* rs = new ResultSet();
        int count = 0;
    }

#if PrintLog == 1
    std::cout << " [DB] SCAN ";
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << " ";
#endif
    
    twait {
        for (it->Seek(start); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key >= limit) break;
            if (strncmp(key.c_str(), start.c_str(), 10) != 0) continue;
            rs->push_back(Result(key, it->value().ToString()));
            count++;
        }
        assert(it->status().ok());
        delete it;
    }

#if PrintLog == 1
    std::cout << count << std::endl;    
    //fflush(stdout);
#endif
    done(*rs);
}

void RocksDBStore::flush() {return;}
void RocksDBStore::run_monitor(Server& server) {return;}
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
        int count = 0;
    }

    twait { pool_->execute(q, make_event(j)); }

    ResultSet& rs = done.result();
    for (auto it = j.abegin(); it < j.aend(); ++it ) {
        rs.push_back(Result((*it)[0].as_s(), (*it)[1].as_s()));
        count++;
    }

    std::cout << "[DB] SCAN "; // DB log
    first.PrintHex(); std::cout << " " << first.length() << " ";
    last.PrintHex();  std::cout << " " <<  last.length() << " ";
    std::cout << count << std::endl;  fflush(stdout);

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
