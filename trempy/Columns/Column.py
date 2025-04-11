class Column:
    """
    Classe que representa uma coluna de uma tabela.

    Attributes:
        name (str): O nome da coluna.
        data_type (str): O tipo de dados da coluna.
        udt_name (str): O nome do tipo de dados personalizado (UDT) da coluna.
        character_maximum_length (int): O tamanho máximo do caractere da coluna.
        nullable (bool): Indica se a coluna pode ser nula.
        ordinal_position (int): A posição ordinal da coluna na tabela.
        is_primary_key (bool): Indica se a coluna é uma chave primária.
    """

    def __init__(
        self,
        name: str = None,
        data_type: str = None,
        udt_name: str = None,
        character_maximum_length: int = None,
        nullable: bool = None,
        ordinal_position: int = None,
        is_primary_key: bool = False,
    ):
        self.name = name
        self.data_type = (
            data_type if data_type != "USER-DEFINED" else "character varying"
        )
        self.udt_name = udt_name
        self.character_maximum_length = character_maximum_length
        self.nullable = nullable
        self.ordinal_position = ordinal_position
        self.is_primary_key = is_primary_key

        self.id = f"{self.name}"
