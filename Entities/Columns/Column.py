class Column:
    def __init__(self,
                 name: str = None,
                 data_type: str = None,
                 udt_name: str = None,
                 character_maximum_length: int = None,
                 nullable: bool = None,
                 ordinal_position: int = None,
                 is_primary_key: bool = False
                 ):
        self.name = name
        self.data_type = data_type if data_type != 'USER-DEFINED' else 'character varying'
        self.udt_name = udt_name
        self.character_maximum_length = character_maximum_length
        self.nullable = nullable
        self.ordinal_position = ordinal_position
        self.is_primary_key = is_primary_key

        self.id = f'{self.name}'