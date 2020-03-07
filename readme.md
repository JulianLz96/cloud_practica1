## How to use

**Requeriments**

boto3
nltk
___
### loadImages.py
_It only records 100 data's rows in the databases_
in root use: `py loadImages.py`

### queryImages.py
_It searchs a image with a label asociate_
in root use: `py queryImage.py <query> <query2> <etc>` 
example: `py queryImage.py america south`

### dynamostorage
_It creates in your profile 2 databases and fill with num of rows like conidition_
in root use: `py dynamostorage.py <numOfRowsToProcess>`
example: `py dynamostorage.py 200`