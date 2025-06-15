'''An extra day is added to the calendar almost every four years as February 29, and the day is called a leap day. 
It corrects the calendar for the fact that our planet takes approximately 365.25 days to orbit the sun. 
A leap year contains a leap day.'''

def is_leap(year):
    leap = False
    if (year % 4 == 0):
        if (year % 100 == 0):
            if (year % 400 == 0):
                return True
            else:
                return False
        else:
            return True
    else:
        return False
    
    return leap

year = int(input())
print(is_leap(year))