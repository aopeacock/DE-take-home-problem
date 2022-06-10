Solution is under the solution directory and file name is load_user_logins.py .

To run the program load_user_logins.py needs to be exectuted after the environment has been set up. This script will pull the data from Amazon SQS queue, apply the transformations, and load the data to the user_logins table.

python load_user_logins.py

Next steps:
Connections to postgres DB and Amazon SQS could be broken out into their own files. A config also needs to be setup with the postgres credentials and included in .gitignore so they are not shared with others.
Test cases need to be setup to compare data between source and target to ensure it was loaded and transformations were applied correctly. Record counts could also be compared to ensure no records are missing.
Further research on if there is a better way to mask the PII fields than the sha256 hashing algo used. 
Research if there is a more effecient way of storing the data instead of using a list with a nested dictionary for each record.
Try/Except blocks need to be included around different sections of code to catch errors.


Notes:
The user_logins create table statement was altered to change app_version to a varchar(32) instead of integer data type. There were records with values like 0.3.2 that would not convert to an integer during the transformation phase. This was an assumption I made changing the data type. Alternatively I could have stripped the periods from the values and inserted only the digits into the field if the field needs to be kept as an integer data type.
Also ran into some environment setup issues on windows. (the joy of trying to use windows to work the problem :D ) A dos2unix command was executed on the 01_call_python_scripts.sh bash script to get around them. 