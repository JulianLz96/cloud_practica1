import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import sys


# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("db/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("db/sqlite_images.db","images")

# Process Logic.
command_querys=[]
images=[]
category=""
image=""
sys.argv.pop(0)
for query in sys.argv:
    stemmed_query = Stemmer.stem(query)
    categorys = kv_labels.getAll(stemmed_query)
    print(categorys[0])
    if categorys is not None:
        for category in categorys:
            image = kv_images.get(str(category[0]))
            if image is not None:
                images.append(image)

print(images)
# Close KeyValues Storages
kv_labels.close()
kv_images.close()







