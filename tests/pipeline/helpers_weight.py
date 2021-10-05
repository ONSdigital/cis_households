"""Provides a random choice from a given distribution"""
from typing import Any
from typing import Callable
from typing import List
from typing import Tuple

import numpy as np
from mimesis.providers.base import BaseDataProvider


class BaseStatsDataProvider(BaseDataProvider):
    """
    Class for all mimesis_stats providers to inherit.
    Allows access to generic _replace() across all providers and
    consistent random seeding.
    Notes
    -----
    Sets global seed for numpy.
    """

    class Meta:
        name = "base_stats"

    def __init__(self, *args: Any, **kwargs: Any) -> None:

        super().__init__(*args, **kwargs)

        np.random.seed(self.seed)

    @staticmethod
    def _replace(value: Any, proportion: float = 0.0, replacement: Any = None) -> Any:
        """
        Replace value with given probability.
        Normally used with a None replacement.
        Parameters
        ----------
        value
            Value that may be replaced with Null value
        proportion
            Probability of individual replacement with null
            Matches overall proportion null desired at large sample size
        replacement
            The null or otherwise value that will replace the input given
            the probability.
        Returns
        -------
        value or null
        Notes
        -----
        Defaults cause no replacement
        """
        if not proportion:
            return value

        if np.random.uniform(size=None) < proportion:
            return replacement
        else:
            return value

    def _replace_multiple(self, values: Tuple[Any], proportions: List[float], replacements: Any) -> Tuple[Any, ...]:
        """
        Adaptation of self._replace() for multiple values.
        Replace values with given probability.
        Normally used with a None replacement.
        Parameters
        ----------
        values
            Values that may be replaced with Null value
        proportion
            Probability of individual replacement with null for each element in values.
            Matches overall proportion null desired at large sample size
        replacement
            The null or otherwise value that will replace the input for the corresponding element given
            the probability.
        Returns
        -------
        Tuple[value or null for each value]
        Notes
        -----
        Defaults cause no replacements
        """
        if not proportions:
            return values

        if replacements is None:
            replacements = [None] * len(values)

        value_triplets = zip(values, proportions, replacements)
        return tuple(
            self._replace(element, proportion, replacement) for element, proportion, replacement in value_triplets
        )


class Distribution(BaseStatsDataProvider):
    """
    Class for univariable distribution sampling.
    Methods
    -------
    discrete_distribution
        Discrete choices (categorical-type) variables
    generic_distribution
        Accepts functions for custom distribution.
    """

    class Meta:
        name = "distribution"

    def __init__(self, *args: Any, **kwargs: Any) -> None:

        super().__init__(*args, **kwargs)

    def generic_distribution(self, func: Callable, null_prop: float = 0, null_value: Any = None, **kwargs: Any) -> Any:
        """
        Draw from any distribution passed by a function.
        Replace a proportion with None values.
        Parameters
        ----------
        func
            Function defining the distribution
            Expected to return a single value
        null_prop
            Proportion of values to replace as null
        null_value
            The (null) value to replace a sample with
        **kwargs
            Keyword arguments needed for func distribution
        Returns
        -------
        Single value from func call with kwargs
        Examples
        --------
        >>>Distribution.generic_distribution(func=np.random.normal, loc=1, null_prop=0.0)
        1.06
        >>>Distribution.generic_distribution(func=stats.bernoulli.rvs, p=0.3, loc=2)
        2
        """
        return self._replace(func(**kwargs), null_prop, replacement=null_value)

    def discrete_distribution(
        self, population: List[Any], weights: List[float], null_prop: float = 0, null_value: Any = None
    ) -> Any:
        """
        Draw from discrete fix-proportion distribution.
        Replace a proportion with null_value.
        Parameters
        ----------
        population
            The values to sample from
        weights
            Probabilities to weight the sampling, index matched with population
        null_prop
            Proportion of values to replace as null
        null_value
            The (null) value to replace a sample with
        Returns
        -------
        Single element from population or null_value
        Examples
        --------
        >>>Distribution.distrete_distribution(population=["one", "two", "three"], weights=[0.01, 0.01, 0.98])
        "three"
        """
        return self._replace(
            np.random.choice(population, size=None, p=weights, replace=False), null_prop, replacement=null_value
        )
