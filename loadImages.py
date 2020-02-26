import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer


# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("db/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("db/sqlite_images.db","images")

# Process Logic.
images = []
terms = []

temp = 0
images_ttl = ParseTripe.ParseTriples("images.ttl") 
labels_ttl = ParseTripe.ParseTriples("labels_en.ttl")

while temp < 1:
    temp_images = images_ttl.getNext()
    temp_labels = labels_ttl.getNext()
    if(temp_images[1] == "http://xmlns.com/foaf/0.1/depiction"):
        temp_arr = [temp_images[0], temp_images[2]]
        images.append(temp_arr)
    if(temp_labels[1] == "http://www.w3.org/2000/01/rdf-schema#label"):
        len_labels = temp_labels[2].split(" ")
        if(len(len_labels) > 1):
            for label in len_labels:
                temp_arr = [Stemmer.stem(label),     temp_labels[0]]
                terms.append(temp_arr)
        else:
            temp_arr = [temp_labels[2], temp_labels[0]]
            terms.append(temp_arr)
    temp+=1

print(images)

# Close KeyValues Storages
kv_labels.close()
kv_images.close()







