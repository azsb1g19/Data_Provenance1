import multiprocessing
import pymongo
import multiprocessing
import itertools
import tqdm
import os
from itertools import islice
import csv
from statistics import mean

def chunk(it, size):
    it = iter(it)
    if size == 0:
        return map(lambda i: [i], it)
    return iter(lambda: tuple(islice(it, size)), ())

def strip_row_from_str(str):
    stripped_str = str.split("^^")
    stripped_str[2] = stripped_str[2].replace('Row','')
    return "^^".join(stripped_str)

def clean_db_activities(zip_of_docs_and_db_name):
    client = pymongo.MongoClient('localhost', 27017)   
    docs, db_name = zip_of_docs_and_db_name
    collection = client[db_name]['activities']
    for doc in docs:
        if 'attributes' in doc:
            attributes = doc['attributes']
            if 'used_features' in attributes:
                used_features = attributes['used_features']
                if isinstance(used_features, list):
                    used_features.sort()
                    collection.update_one(
                        {
                            '_id': doc['_id']
                        },
                        {
                            "$set": {'attributes.used_features': used_features}
                        }
                    )

def clean_db_derivations(zip_of_docs_and_db_name):
    client = pymongo.MongoClient('localhost', 27017)   
    docs, db_name = zip_of_docs_and_db_name
    collection = client[db_name]['derivations']
    for doc in docs:
        set_ = {}
        if 'gen' in doc:
            set_.update({'gen': strip_row_from_str(doc['gen'])})
        if 'used' in doc:
            set_.update({'used': strip_row_from_str(doc['used'])})
        if 'id' in doc:
            set_.update({'id': strip_row_from_str(doc['id'])})
        collection.update_one(
            {
                '_id': doc['_id']
            },
            {
                "$set": set_
            }
        )

def clean_db_entities(zip_of_docs_and_db_name):
    client = pymongo.MongoClient('localhost', 27017)   
    docs, db_name = zip_of_docs_and_db_name
    collection = client[db_name]['entities']
    for doc in docs:
        collection.update_one(
            {
                '_id': doc['_id']
            },
            {
                "$set": {'id':strip_row_from_str(doc['id'])}
            }
        )

def clean_db_relations(zip_of_docs_and_db_name):
    client = pymongo.MongoClient('localhost', 27017)   
    docs, db_name = zip_of_docs_and_db_name
    collection = client[db_name]['relations']
    while len(docs) > 0:
        doc = docs.pop()
        docs_with_same_id = list(collection.find({"id":doc['id']}))
        if len(docs_with_same_id) > 1: # remove docs with multiple ids because it isn't feasible to test their accuracy
            collection.remove({"id":doc['id']})
        elif len(docs_with_same_id) == 1:
            set_ = {}
            if 'generated' in doc:
                generated = list(map((lambda s: strip_row_from_str(s)), doc['generated']))
                generated.sort()
                set_.update({'generated': generated})
            if 'used' in doc:
                used = list(map((lambda s: strip_row_from_str(s)), doc['used']))
                used.sort()
                set_.update({'used': used})
            if 'invalidated' in doc:
                invalidated = list(map((lambda s: strip_row_from_str(s)), doc['invalidated']))
                invalidated.sort()
                set_.update({'invalidated': invalidated})
            collection.update_one(
                {
                    'id': str(doc['id'])
                },
                {
                    '$set': set_
                }
            )

def does_entity_doc_in_1_exist_in_2(zip_of_docs_in_1_and_name_of_db_2):
    client = pymongo.MongoClient('localhost', 27017)   
    docs_in_1, db_2_name = zip_of_docs_in_1_and_name_of_db_2
    db_2 = client[db_2_name]
    return_array = []
    for doc_in_1 in docs_in_1:
        docs_found_in_2 = list(db_2['entities'].find({"id":doc_in_1['id']}))
        if len(docs_found_in_2) == 1:
            doc_found_in_2 = docs_found_in_2[0]
            expected_keys = ['id', '_id', 'record_id']
            if set(doc_found_in_2.keys()) == set(expected_keys):
                return_array.append(1)
                continue
        return_array.append(0)
        continue
    return return_array

def does_derivation_doc_in_1_exist_in_2(zip_of_docs_in_1_and_name_of_db_2):
    client = pymongo.MongoClient('localhost', 27017)   
    docs_in_1, db_2_name = zip_of_docs_in_1_and_name_of_db_2
    db_2 = client[db_2_name]
    return_array = []
    for doc_in_1 in docs_in_1:
        find_obj = {}
        if 'gen' in doc_in_1:
            find_obj.update({"gen": doc_in_1["gen"]})
        if 'used' in doc_in_1:
            find_obj.update({"used": doc_in_1["used"]})
        if find_obj == {}:
            return_array.append(1)
        else:
            docs_found_in_2 = list(db_2['derivations'].find(find_obj))
            if len(docs_found_in_2) == 1:
                doc_found_in_2 = docs_found_in_2[0]
                expected_keys = list(find_obj.keys())
                expected_keys.append('_id')
                if set(doc_found_in_2.keys()) == set(expected_keys):
                    return_array.append(1)
                    continue
            return_array.append(0)
    return return_array

def does_activity_doc_in_1_exist_in_2(zip_of_docs_in_1_and_name_of_db_2):
    client = pymongo.MongoClient('localhost', 27017)   
    docs_in_1, db_2_name = zip_of_docs_in_1_and_name_of_db_2
    return_array = []
    for doc_in_1 in docs_in_1:
        db_2 = client[db_2_name]
        docs_found_in_2 = list(db_2['activities'].find({"attributes":doc_in_1['attributes']}))
        if len(docs_found_in_2) == 1:
            doc_found_in_2 = docs_found_in_2[0]
            expected_attribute_keys = list(doc_in_1["attributes"].keys())
            if set(doc_found_in_2["attributes"].keys()) == set(expected_attribute_keys):
                return_array.append(1)
                continue
        return_array.append(0)
        continue
    return return_array

def does_relation_doc_in_1_exist_in_2(zip_of_docs_in_1_and_name_of_db_2):
    client = pymongo.MongoClient('localhost', 27017)   
    docs_in_1, db_2_name = zip_of_docs_in_1_and_name_of_db_2
    return_array = []
    for doc_in_1 in docs_in_1:
        find_obj = {}
        if 'generated' in doc_in_1:
            find_obj.update({"generated": doc_in_1["generated"]})
        if 'used' in doc_in_1:
            find_obj.update({"used": doc_in_1["used"]})
        if 'invalidated' in doc_in_1: 
            find_obj.update({"invalidated": doc_in_1["invalidated"]})
        db_2 = client[db_2_name]
        if find_obj == {}:
            return_array.append(1)
        else:
            docs_found_in_2 = list(db_2['relations'].find(find_obj))
            if len(docs_found_in_2) >= 1:
                doc_found_in_2 = docs_found_in_2[0]
                expected_keys = list(find_obj.keys())
                expected_keys.extend(['id', '_id'])
                if set(doc_found_in_2.keys()) == set(expected_keys):
                    return_array.append(1)
                else:
                    return_array.append(0)
            else:
                return_array.append(0)
    return return_array

class Tester:
    
    def clean_attrs(self,db_name,attrs):
        client = pymongo.MongoClient('localhost', 27017)   
        print("Cleaning " + attrs + " of " + db_name)
        clean_func_name = "clean_db_" + attrs
        clean_func = globals()[clean_func_name]        
        db = client[db_name]
        docs = list(db[attrs].find({}))
        client.close()
        doc_chunks = list(chunk(docs, round(len(docs) / 1500))) # the maximum number of socket connections to the database is bottlenecked by the TCP/IP stack to about 5000 ports, to be safe we only create a fraction of that many connections
        tasks = zip(doc_chunks, list(itertools.repeat(db_name, len(doc_chunks))))
        list(tqdm.tqdm(self.pool.imap(clean_func, tasks), total=len(doc_chunks)))
        

    def clean_activities(self,db_name):
        self.clean_attrs(db_name, 'activities')

    def clean_derivations(self,db_name):
        self.clean_attrs(db_name, 'derivations')

    def clean_entities(self,db_name):
        self.clean_attrs(db_name, 'entities')

    def clean_relations(self,db_name):
        self.clean_attrs(db_name, 'relations')

    def clean_db(self, db_name):
        print("Cleaning " + db_name)
        self.clean_activities(db_name) 
        self.clean_derivations(db_name) 
        self.clean_entities(db_name)
        self.clean_relations(db_name)

    def clean_dbs(self):
        self.clean_db(self.prov_db_1_name)
        self.clean_db(self.prov_db_2_name)
    
    def create_activities_index_attr(self,db_name):
        client = pymongo.MongoClient('localhost', 27017)   
        print("Creating indexes on activities of " + db_name)
        db = client[db_name]
        db['activities'].create_index("attributes")
        client.close()

    def create_entities_index_attr(self,db_name):
        client = pymongo.MongoClient('localhost', 27017)   
        print("Creating indexes on entities of " + db_name)
        db = client[db_name]
        db['entities'].create_index("id")
        client.close()

    def create_derivations_index_attr(self,db_name):
        client = pymongo.MongoClient('localhost', 27017)   
        print("Creating indexes on derivations of " + db_name)
        db = client[db_name]
        db['derivations'].create_index("gen")
        db['derivations'].create_index("used")
        client.close()

    def create_relations_index_attr(self,db_name):
        client = pymongo.MongoClient('localhost', 27017)   
        print("Creating indexes on relations of " + db_name)
        db = client[db_name]
        db['relations'].create_index("generated")
        db['relations'].create_index("used")
        db['relations'].create_index("invalidated")
        client.close()
    
    def create_indices(self,db_name):
        self.create_activities_index_attr(db_name)
        self.create_entities_index_attr(db_name)
        self.create_derivations_index_attr(db_name)
        self.create_relations_index_attr(db_name)
    
    def create_indices_on_dbs(self):
        self.create_indices(self.prov_db_1_name)
        self.create_indices(self.prov_db_2_name)

    def __init__(self, prov_db_1_name, prov_db_2_name, testing_prov_db_1_name, testing_prov_db_2_name):
        client = pymongo.MongoClient('localhost', 27017)   
        dblist = client.list_database_names()
        if not prov_db_1_name in dblist:
            raise Exception(prov_db_1_name + " not in MongoDB")
        if not prov_db_2_name in dblist:
            raise Exception(prov_db_2_name + " not in MongoDB")
        self.prov_db_1_name = testing_prov_db_1_name
        self.prov_db_2_name = testing_prov_db_2_name
        dblist = client.list_database_names()
        if testing_prov_db_1_name in dblist:
            print('Dropping existing database', testing_prov_db_1_name)
            client.drop_database(testing_prov_db_1_name)
        if testing_prov_db_2_name in dblist:
            print('Dropping existing database', testing_prov_db_2_name)
            client.drop_database(testing_prov_db_2_name)
        os.system('mongodump --archive="mongodump-prov" --db=' + prov_db_1_name)
        os.system('mongorestore --archive="mongodump-prov" --nsFrom="' + prov_db_1_name + '.*" --nsTo="' + testing_prov_db_1_name + '.*"')
        os.system('mongodump --archive="mongodump-prov" --db=' + prov_db_2_name)
        os.system('mongorestore --archive="mongodump-prov" --nsFrom="' + prov_db_2_name + '.*" --nsTo="' + testing_prov_db_2_name + '.*"')
        self.pool = multiprocessing.Pool(multiprocessing.cpu_count())
        client.close()
        self.clean_dbs()
        self.create_indices_on_dbs() # create indexes after cleaning because some documents get dropped
        self.client = pymongo.MongoClient('localhost', 27017)   
        self.prov_db_1 = self.client[testing_prov_db_1_name]
        self.prov_db_2 = self.client[testing_prov_db_2_name]
        print("Init Complete")
    
    def test_attr(self, attr, attrs):
        db_1_attrs_docs = list(self.prov_db_1[attrs].find({}))
        n_attr_docs_in_db_1 = len(db_1_attrs_docs)
        n_attr_docs_in_db_2 = len(list(self.prov_db_2[attrs].find({})))

        if n_attr_docs_in_db_1 == 0 and n_attr_docs_in_db_2 == 0:
            return 100.0
        if n_attr_docs_in_db_1 == 0 and n_attr_docs_in_db_2 != 0:
            return 0.0

        doc_chunks = list(chunk(db_1_attrs_docs, round(len(db_1_attrs_docs) / 2250))) # the maximum number of socket connections to the database is bottlenecked by the TCP/IP stack to about 5000 ports, to be safe we only create half that many connections
        tasks = zip(doc_chunks, list(itertools.repeat(self.prov_db_2_name, len(doc_chunks))))
        matching_func_name = "does_" + attr + "_doc_in_1_exist_in_2"
        matching_func = globals()[matching_func_name]
        print("Testing", attrs) # note that, as attributes are tested, documents found in the second database are removed to lower the search time for successive searches - this reduces the search time by half
        completed_tasks = list(itertools.chain(*list(tqdm.tqdm(self.pool.imap_unordered(matching_func, tasks), total=len(doc_chunks))))) # pool imap returns a 2d array of 1 and 0s, so we flatten back to 1d
        matching_attrs = sum(completed_tasks)

        n_attr_docs_in_db_2_not_in_1 = n_attr_docs_in_db_2 - n_attr_docs_in_db_1
        if n_attr_docs_in_db_2_not_in_1 < 0:
            n_attr_docs_in_db_2_not_in_1 = 0

        foo = matching_attrs - n_attr_docs_in_db_2_not_in_1
        if foo <= 0:
            foo = 0

        percentage_of_matching_attrs = (foo / len(completed_tasks)) * 100
        return percentage_of_matching_attrs        

    def test_activities(self):
        return self.test_attr('activity', 'activities')

    def test_derivations(self):
        return self.test_attr('derivation', 'derivations')

    def test_entities(self):
        return self.test_attr('entity', 'entities')
    
    def test_relations(self):
        return self.test_attr('relation', 'relations')
    
    def close(self):
        self.client.close()

if __name__ == "__main__":
    prov_db_pairs_list = [
        ("left_join_py_db", "left_join_knime_db"),
        ("right_join_py_db", "right_join_knime_db"),
        ("inner_join_py_db", "inner_join_knime_db"),
        ("outer_join_py_db", "outer_join_knime_db"),
        ("cross_join_py_db", "cross_join_knime_db"),
        ("german_py_db", "german_knime_db"),
        ("compas_py_db", "compas_knime_db"),
    ]

    prog_dir = os.path.dirname(os.path.realpath(__file__))
    save_path = os.path.join(prog_dir, "metrics.csv")

    if os.path.isfile(save_path):
        os.remove(save_path)
    with open(save_path, mode='w', newline='') as file:
        csv.writer(file).writerow([
            "db_1_name",
            "db_2_name",
            "average_similarity",
            "similarity_of_entities",
            "similarity_of_activities",
            "similarity_of_derivations",
            "similarity_of_relations",
        ])

    testing_prov_db_1_name = 'testing_prov_db_1'
    testing_prov_db_2_name = 'testing_prov_db_2'

    for prov_db_1_name, prov_db_2_name in prov_db_pairs_list:
        print("Testing:", prov_db_1_name, "and", prov_db_2_name)
        tester = Tester(prov_db_1_name, prov_db_2_name, testing_prov_db_1_name, testing_prov_db_2_name)
        similarity_of_entities = tester.test_entities()
        print("similarity_of_entities", similarity_of_entities)
        similarity_of_activities = tester.test_activities()
        print("similarity_of_activities", similarity_of_activities)
        similarity_of_derivations = tester.test_derivations()
        print("similarity_of_derivations", similarity_of_derivations)
        similarity_of_relations = tester.test_relations()
        print("similarity_of_relations", similarity_of_relations)
        tester.close()
        average_similarity = mean([similarity_of_entities, similarity_of_activities, similarity_of_derivations, similarity_of_relations])
        print("average_similarity", average_similarity)
        with open(save_path, mode='a', newline='') as file:
            csv.writer(file).writerow([
                prov_db_1_name,
                prov_db_2_name,
                average_similarity,
                similarity_of_entities,
                similarity_of_activities,
                similarity_of_derivations,
                similarity_of_relations,
            ])