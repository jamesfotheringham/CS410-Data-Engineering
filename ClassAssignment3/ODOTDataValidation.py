import pandas as pd
import ValidationFunctions as vf

# Read in crash data
df = pd.read_csv("Hwy26Crashes2019.csv", dtype={'Age': object, 'Crash ID': object, 'Crash Day': object, 'Crash Month': object, 'Crash Year': object, 'County Code': object})

# Separate crash data into three separate dataframes corresponding to the three tables of data
CrashesDF = df[df['Record Type'] == 1]
VehiclesDF = df[df['Record Type'] == 2]
ParticipantsDF = df[df['Record Type'] == 3]

CrashesDF = CrashesDF.dropna(axis=1, how='all')
VehiclesDF = VehiclesDF.dropna(axis=1, how='all')
ParticipantsDF = ParticipantsDF.dropna(axis=1, how='all')

# Assert that every record has a crashID
try:
    crashIdColumn = df['Crash ID']
    validatedRecords = crashIdColumn.apply(vf.validate_existence)
    assert validatedRecords.isnull().values.any() == False
except AssertionError:
    print("Validation of Crash ID Records Failed")
    if (validatedRecords.isnull().values.all() == True):
        print("Whole Column is Faulty - Dropping Column")
        df = df.drop(columns=['Crash ID'])
    else:
        print("Dropping Faulty Rows")
        df['Crash ID'] = validatedRecords
        df = df.dropna(subset=['Crash ID']) 

# Assert that every record type of 1 has a County Code 
try:
    CountyCodeColumn = CrashesDF['County Code']
    validatedRecords = CountyCodeColumn.apply(vf.validate_existence)
    assert validatedRecords.isnull().values.any() == False
except AssertionError:
    print("Validation of County Code Records Failed")
    if (validatedRecords.isnull().values.all() == True):
        print("Entire Column is Faulty - Dropping Column")
        CrashesDF = CrashesDF.drop(columns=['County Code'])
    else:
        print("Dropping Faulty Rows")
        CrashesDF['County Code'] = validatedRecords
        CrashesDF = CrashesDF.dropna(subset=['County Code'])

# Assert each value in age column is two digit values between 00 and 99
try:
    AgeColumn = ParticipantsDF['Age']
    validatedAges = AgeColumn.apply(vf.validate_age)
    assert validatedAges.isnull().values.any() == False
except AssertionError:
    print("Validation of Age Column Failed")
    if(validatedRecords.isnull().values.all() == True):
        print("Entire Column is Faulty - Dropping Column")
        ParticipantsDF = ParticipantsDF.drop(columns=['Age'])
    else:
        print("Dropping Faulty Rows")
        ParticipantsDF['Age'] = validatedAges
        ParticipantsDF = ParticipantsDF.dropna(subset=['Age'])

# Assert every record of type 1 has a crash date in the form DDMMYYYY
try:
    CrashDayColumn = CrashesDF['Crash Day']
    CrashMonthColumn = CrashesDF['Crash Month']
    CrashYearColumn = CrashesDF['Crash Year']

    validatedDays = CrashDayColumn.apply(vf.validate_crash_day_month)
    validatedMonths = CrashMonthColumn.apply(vf.validate_crash_day_month)
    validatedYears = CrashYearColumn.apply(vf.validate_year)

    concatenatedDate = validatedMonths.astype(str) + validatedDays.astype(str) + validatedYears.astype(str)
    validatedDates = concatenatedDate.apply(vf.validate_date)
    print(validatedDates)
    assert validatedDates.isnull().values.any() == False
except AssertionError:
    print("Validation of Date Records Failed - Dropping faulty rows")
    CrashesDF['Crash Day'] = validatedDays
    CrashesDF['Crash Month'] = validatedMonths
    CrashesDF['Crash Year'] = validatedYears
    CrashesDF = CrashesDF.dropna(subset=['Crash Day', 'Crash Month', 'Crash Year'])
    