from trempy.Shared.Types import FilterType, InputFilterType
from typing import Dict, Tuple, Optional, List


class FilterDefinitions:
    """Classe que estrutura o mapeamento entre tipos de filtro e seus inputs necessários."""

    # Mapeamento completo dos filtros para seus tipos de input
    __FILTER_INPUT_MAPPING: Dict[FilterType, Tuple[Optional[InputFilterType], ...]] = {
        # Filtros de valor único
        FilterType.EQUALS: (InputFilterType.VALUE,),
        FilterType.NOT_EQUALS: (InputFilterType.VALUE,),
        FilterType.GREATER_THAN: (InputFilterType.VALUE,),
        FilterType.GREATER_THAN_OR_EQUAL: (InputFilterType.VALUE,),
        FilterType.LESS_THAN: (InputFilterType.VALUE,),
        FilterType.LESS_THAN_OR_EQUAL: (InputFilterType.VALUE,),
        FilterType.STARTS_WITH: (InputFilterType.VALUE,),
        FilterType.ENDS_WITH: (InputFilterType.VALUE,),
        FilterType.CONTAINS: (InputFilterType.VALUE,),
        FilterType.NOT_CONTAINS: (InputFilterType.VALUE,),
        FilterType.DATE_EQUALS: (InputFilterType.VALUE,),
        FilterType.DATE_NOT_EQUALS: (InputFilterType.VALUE,),
        FilterType.DATE_GREATER_THAN: (InputFilterType.VALUE,),
        FilterType.DATE_GREATER_THAN_OR_EQUAL: (InputFilterType.VALUE,),
        FilterType.DATE_LESS_THAN: (InputFilterType.VALUE,),
        FilterType.DATE_LESS_THAN_OR_EQUAL: (InputFilterType.VALUE,),
        # Filtros de múltiplos valores
        FilterType.IN: (InputFilterType.VALUES,),
        FilterType.NOT_IN: (InputFilterType.VALUES,),
        # Filtros de intervalo
        FilterType.BETWEEN: (InputFilterType.LOWER, InputFilterType.UPPER),
        FilterType.NOT_BETWEEN: (InputFilterType.LOWER, InputFilterType.UPPER),
        FilterType.DATE_BETWEEN: (InputFilterType.LOWER, InputFilterType.UPPER),
        FilterType.DATE_NOT_BETWEEN: (InputFilterType.LOWER, InputFilterType.UPPER),
        # Filtros sem input
        FilterType.IS_NULL: tuple(),
        FilterType.IS_NOT_NULL: tuple(),
    }

    @classmethod
    def get_input_types(cls, filter_type: FilterType) -> Tuple[InputFilterType, ...]:
        """Retorna os tipos de input necessários para um determinado filtro.

        Args:
            filter_type: Tipo do filtro conforme definido no enum FilterType.

        Returns:
            Tupla com os tipos de input necessários (pode ser vazia para filtros sem input).

        Raises:
            ValueError: Se o filter_type não for encontrado no mapeamento.
        """
        if filter_type not in cls.__FILTER_INPUT_MAPPING:
            raise ValueError(f"Tipo de filtro não mapeado: {filter_type}")

        return cls.__FILTER_INPUT_MAPPING[filter_type]

    @classmethod
    def get_all_filters_by_input_type(
        cls, input_type: InputFilterType
    ) -> List[FilterType]:
        """Retorna todos os filtros que utilizam um determinado tipo de input.

        Args:
            input_type: Tipo de input conforme definido no enum InputFilterType.

        Returns:
            Lista de FilterTypes que utilizam o tipo de input especificado.
        """
        return [
            ft
            for ft, input_types in cls.__FILTER_INPUT_MAPPING.items()
            if input_type in input_types
        ]

    @classmethod
    def get_filters_without_input(cls) -> List[FilterType]:
        """Retorna todos os filtros que não requerem input de valores.

        Returns:
            Lista de FilterTypes que não necessitam de input.
        """
        return [
            ft
            for ft, input_types in cls.__FILTER_INPUT_MAPPING.items()
            if not input_types
        ]

    @classmethod
    def validate_inputs_for_filter(
        cls, filter_type: FilterType, provided_inputs: Dict[InputFilterType, any]
    ) -> bool:
        """Valida se os inputs fornecidos são adequados para o tipo de filtro.

        Args:
            filter_type: Tipo do filtro a ser validado.
            provided_inputs: Dicionário com os inputs fornecidos.

        Returns:
            True se os inputs estão corretos, False caso contrário.
        """
        required_inputs = cls.get_input_types(filter_type)

        # Verifica se todos os inputs necessários foram fornecidos
        for required in required_inputs:
            if required not in provided_inputs or provided_inputs[required] is None:
                return False

        # Verifica se não há inputs extras não necessários
        provided_types = set(provided_inputs.keys())
        required_types = set(required_inputs)
        return provided_types.issubset(required_types)
