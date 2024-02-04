import pandas as pd

INPUT_DTYPES = {
    'OWNER_COUNTRY': 'string',
    'OWNER_ADDR1': 'string',
    'OWNER_ADDR2': 'string',
    'OWNER_ADDR3': 'string'
}

GOVERNMENT_ENTITIES = [
    'CITY OF WILMINGTON',
    'NEW HANOVER COUNTY',
    'CAPE FEAR PUBLIC UTILITY AUTHORITY',
    'NC STATE OF',
    'WILMINGTON HOUSING AUTHORITY',
    'NC DEPT OF TRANSPORTATION',
    'NEW HANOVER COUNTY AIRPORT AUTHORITY',
    'CAROLINA BEACH TOWN OF',
    'NEW HANOVER COUNTY BOARD OF EDUCATION',
    'DEPARTMENT OF TRANSPORTATION',
    'WRIGHTSVILLE BEACH TOWN OF',
    'NEW HANOVER COUNTY WATER /SEWER AUTH',
    'NC STATE PORTS AUTHORITY',
]

COMMERCIAL_ENDINGS = [
    'LLC',
    'INC',
    'CORP',
    'ASSOC',
    'CORPORATION'
]


class LocalCSVProcessor:
    def __init__(self, resource) -> None:
        self.resource = resource

    def _is_owner_potentially_commercial(self, owner) -> bool:
        for ending in COMMERCIAL_ENDINGS:
            if owner.endswith(f' {ending}'):
                return True
        return False

    def _get_value_count_stats(self, df, column, top_n=10):
        total_records = df.shape[0]
        value_counts = df.value_counts(column).nlargest(top_n)
        value_percents = value_counts.apply(lambda x: x / total_records * 100)
        frame = {
            'count': value_counts,
            'percent': value_percents
        }
        return pd.DataFrame(frame)

    def process(self) -> None:
        df = pd.read_csv(self.resource, dtype=INPUT_DTYPES)

        total_lots = df.shape[0]
        print(f'{total_lots} total lots')
        print()
        print()

        # Filter incorporated lots only
        # incorporated_records = df[df['OWNER_CITY'] == 'WILMINGTON']

        # Get overall stats for zoning class by lot count
        zoning_class_stats = self._get_value_count_stats(df, 'CLASS')
        print('BY ZONING CLASS')
        print(zoning_class_stats)
        print()
        print()

        # Get overall stats for zoning type by lot count
        zoning_type_stats = self._get_value_count_stats(df, 'ZONING')
        print('BY ZONING TYPE')
        print(zoning_type_stats)
        print()
        print()

        # Get owner stats by lot count
        owner_stats = self._get_value_count_stats(df, 'OWN1', top_n=15)
        print('TOP OWNERS (INCLUDING GOVERNMENT ENTITIES)')
        print(owner_stats)
        print()
        print()

        # Get owner (non government entities) by lot count
        d1 = df[~df['OWN1'].isin(GOVERNMENT_ENTITIES)]
        owner_stats = self._get_value_count_stats(d1, 'OWN1', top_n=15)
        print('TOP OWNERS (EXCLUDING GOVERNMENT ENTITIES)')
        print(owner_stats)
        print()
        print()

        # Filter by residential zoned lots only
        d1 = pd.DataFrame(df[df['CLASS'] == 'RES'])
        residential_lot_count = d1.shape[0]
        print(f'{residential_lot_count} residential lots')
        d1['potentially_commercial'] = d1.apply(lambda x: self._is_owner_potentially_commercial(x['OWN1']), axis=1)
        d2 = d1[d1['potentially_commercial']]
        commercial_residential_lot_count = d2.shape[0]
        print(f'{commercial_residential_lot_count} residential lots owned by commercial entities')
        commercial_percent = commercial_residential_lot_count / residential_lot_count * 100
        print(f'{commercial_percent:.2f}% of residential zoned lots owned by commercial entities')
        print()
        print()

        owner_stats = self._get_value_count_stats(d2, 'OWN1')
        print('TOP COMMERCIAL OWNERS OF RESIDENTIAL LOTS')
        print(owner_stats)


if __name__ == '__main__':
    processor = LocalCSVProcessor('../resources/nhc-01-2024.csv')
    processor.process()
