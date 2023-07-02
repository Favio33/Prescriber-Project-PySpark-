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

factTable = {
    'npi': {
        'type': str,
        'maxLength': 20,
        'alias': 'presc_id'
    },
    'nppes_provider_last_org_name': {
        'type': str,
        'maxLength': 2,
        'alias': 'presc_lname'
    },
    'nppes_provider_first_name': {
        'type': str,
        'maxLength': 100,
        'alias': 'presc_fname'
    },
    'nppes_provider_city': {
        'type': str,
        'maxLength': 100,
        'alias': 'presc_city'
    },
    'nppes_provider_state': {
        'type': str,
        'maxLength': 100,
        'alias': 'presc_state'
    },
    'specialty_description': {
        'type': str,
        'maxLength': 100,
        'alias': 'presc_spclt'
    }
    ,
    'years_of_exp': {
        'type': str,
        'maxLength': 100,
        'alias': 'years_of_exp'
    }
    ,
    'drug_name': {
        'type': str,
        'maxLength': 100,
        'alias': 'drug_name'
    }
    ,
    'total_claim_count': {
        'type': int,
        'alias': 'trx_cnt'
    }
    ,
    'total_day_supply': {
        'type': str,
        'maxLength': 100,
        'alias': 'total_day_supply'
    }
    ,
    'total_drug_cost': {
        'type': float,
        'alias': 'total_drug_cost'
    }
}

reportPrescriber = ['presc_id', 'full_name', 'presc_state', 'country_name', 'years_of_exp', 'trx_cnt',
                    'total_day_supply', 'total_drug_cost']