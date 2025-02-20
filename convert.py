from decimal import Decimal

def remove_comma(input_string):
    return Decimal(input_string.replace(",", ""))

def period2comma(input_string):
    return input_string.replace(".", ",")

def comma2period(input_string):
    return input_string.replace(",", ".")