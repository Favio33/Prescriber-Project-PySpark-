from pyspark.sql import functions as f


dimensionsTable = {
    'city': {
        'type': str,
        'maxLength': 20
    },
    'state_id': {
        'type': str,
        'maxLength': 2,
    },
    'state_name': {
        'type': str,
        'maxLength': 100
    },
    'county_name': {
        'type': str,
        'maxLength': 100
    },
    'population': {
        'type': int,
    },
    'zips': {
        'type': 'glosario',
        'subtype': int
    }
}
