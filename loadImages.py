import sys
sys.path.append('C:/ITESO/Cloud/Practicas/cloud_practica1/keyvalue')
import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("db/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("db/sqlite_images.db","images")

# Process Logic.
images = []
terms = []
temp_cat = []

temp = 0
images_ttl = ParseTripe.ParseTriples("resources/images.ttl") 
labels_ttl = ParseTripe.ParseTriples("resources/labels_en.ttl")

while temp < 100:
    temp_images = images_ttl.getNext()
    if(temp_images[1] == "http://xmlns.com/foaf/0.1/depiction"):
        temp_arr = [temp_images[0], temp_images[2]]
        images.append(temp_arr)
        kv_images.put(str(temp_images[0]),str(temp_images[2]))
        if not temp_images[0] in temp_cat:
            temp_cat.append(temp_images[0])
    temp+=1
temp=0
while temp < 100:
    temp_labels = labels_ttl.getNext()
    if(temp_labels[1] == "http://www.w3.org/2000/01/rdf-schema#label"):
        len_labels = temp_labels[2].split(" ")
        sort_value=temp_labels[0].split("/")[4]
        if(len(len_labels) > 1):
            for label in len_labels:
                if temp_labels[0] in temp_cat:
                    stemmed_word=Stemmer.stem(label)
                    temp_arr = [stemmed_word,sort_value, temp_labels[0]]
                    terms.append(temp_arr)
                    kv_labels.putSort(str(stemmed_word),str(sort_value),str(temp_labels[0]))
        else:
            temp_arr = [temp_labels[2],sort_value,temp_labels[0]]
            terms.append(temp_arr)
            kv_labels.putSort(str(stemmed_word),str(sort_value),str(temp_labels[0]))
    temp+=1
    
print("Labels processed")

# Close KeyValues Storages
kv_labels.close()
kv_images.close()







