import pandas as pd


class LocalCSVProcessor:
    def __init__(self, resource) -> None:
        self.resource = resource

    def _is_owner_potentially_commercial(self, owner) -> bool:
        print(owner)
        return any([
            owner.endswith(' LLC'),
            owner.endswith(' INC'),
        ])

    def process(self) -> None:
        df = pd.read_csv(self.resource)

        total_records = df.shape[0]
        print(f'{total_records} records processed')

        # Filter incorporated lots only
        # incorporated_records = df[df['OWNER_CITY'] == 'WILMINGTON']

        # Get overall stats for zoning type
        zoned_map = df['CLASS'].value_counts()
        zoned_map = pd.DataFrame(zoned_map)
        zoned_map = zoned_map.assign(percent_total=lambda x: x['CLASS'] / total_records * 100)
        print(zoned_map)

        # Filter by residential zoned lots only
        df = df[df['CLASS'] == 'RES']
        df['potentially_commercial'] = df.apply(lambda x: self._is_owner_potentially_commercial(x['OWN1']), axis=1)

        print(df[['OWN1', 'potentially_commercial']])


if __name__ == '__main__':
    processor = LocalCSVProcessor('../resources/nhc-09-2022.csv')
    processor.process()
