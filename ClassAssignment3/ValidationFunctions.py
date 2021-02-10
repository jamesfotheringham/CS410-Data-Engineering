def validate_age(age):
    """"
    Description: Validate if age of participant has a value of 00 to 99

    Arguments: (int) age to be validated

    Returns:
        True if age is valid
        False if age is not valid
    """

    try:
        ageVal = float(age)
        ageStr = str(age)
        if ageVal < 0 or ageVal > 99:
            return None
        if len(ageStr) != 2:
            return None
        return age
    except ValueError:
        return None

def validate_existence(ID):
    """"
    Description: Validate that every record has an integer value Crash ID and is not empty

    Args: (int) Value to be validated

    Returns:
        True if exists
        False if does not exist
    """

    try:
        intID = int(ID)
        return ID
    except ValueError:
        return None

def validate_crash_day_month(date):
    """"
    Description: Validate that days or months are two digit values of the form DD or MM

    Args: (int) Date to be validated

    Returns:
        Original value if form is correct,
        DD or MM if the form is D or M, 
        False if date is incorrect value
    """

    try:
        dateStr = str(date)
        if len(dateStr) == 1:
            dateStr = '0' + dateStr
            return dateStr
        if len(dateStr) != 2:
            return None
        return date
    except ValueError:
        return None

def validate_year(year):
    """"
    Description: Validate that years are four digit values of the form YYYY

    Args: (int) Year to be validated

    Returns:
        Original value if year is of the form YYYY
        False if year is incorrect value
    """

    try:
        yearStr = str(year)
        if len(yearStr) != 4:
            return None
        return year
    except ValueError:
        return None

def validate_date(date):
    """"
    Description: Validate that dates are of the form MMDDYYYY

    Args: (int) Date to be validated

    Returns:
        True if year is of the form YYYY
        False if year is incorrect value
    """

    try:
        dateStr = str(date)
        if len(dateStr) != 8:
            return None
        return date
    except ValueError:
        return None

