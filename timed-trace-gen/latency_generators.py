RANDOM_BATCH_SIZE = 10000000

import numpy as np 
from typing import Generator, List, Dict

from math import fsum
from functools import reduce


def verifyDists(cluster_dist: List[float], num_of_generators : int):
    dist_sum: float = fsum(cluster_dist)
    if dist_sum != 1.0 or num_of_generators != len(cluster_dist):
        raise ValueError(f'Bad configuration {dist_sum}, num of generators: {num_of_generators} ' +
                         f'and num of clusters {len(cluster_dist)}')
        

class NormalDist():
    __slots__ = '_std_div', '_random_gen', 'mean', 'index', 'gen_values'
    def __init__(self, mean: float, std_div: float):
        self._std_div = std_div
        self._random_gen = np.random.default_rng()
        
        self.mean = mean

        self.refill_values()
    
    def refill_values(self):
        self.index = 0
        self.gen_values = self._random_gen.normal(self.mean, self._std_div, size=RANDOM_BATCH_SIZE)
        
        for i in range(RANDOM_BATCH_SIZE):
            val = self.gen_values[i]
            self.gen_values[i] = max(val, self.mean - 3 * self._std_div, 5)
    
    def __str__(self):
        return f'Normal with mean {self.mean} and sigma {self._std_div}'


class UniformDist():
    __slots__ = '_low', '_high', '_random_gen', 'mean', 'index', 'gen_values'
    def __init__(self, low: float, high: float):
        self._low = low
        self._high = high
        self._random_gen = np.random.default_rng()
        
        self.mean = (low + high * 1.0) / 2
        self.refill_values()
        
    def refill_values(self):
        self.index = 0
        self.gen_values = self._random_gen.uniform(self._low, self._high, size=RANDOM_BATCH_SIZE)
        
    def __str__(self):
        return f'Uniform between {self._low} and {self._high}'


class MultiplePeaksDist():
    __slots__ = '_values', '_probs', 'mean', 'index', 'gen_values'
    def __init__(self, values : List[float], probs : List[float]):
        if not len(probs) == len(values):
            raise ValueError(f'length mismatch for probs: {probs} and values {values} {len(probs)} != {len(values)}')
        
        if not fsum(probs) == 1:
            raise ValueError(f'Invalid Probabilities - the sum is: {fsum(probs)}')
        
        self._values = values
        self._probs = probs
        
        self.mean = reduce(lambda acc, curr: acc + curr[0] * curr[1], zip(self._values, self._probs), 0)
        self.refill_values()
    
    def refill_values(self):
        self.index = 0
        self.gen_values = np.random.choice(self._values, p=self._probs, size=RANDOM_BATCH_SIZE)
        
    def __str__(self):
        return f'{len(self._values)} Peaks with values {self._values} and probabilty {self._probs}'

     
class SingleValueDist():
    __slots__ = 'mean', 'index', 'gen_values'
    def __init__(self, val: float):
        self.mean = val
        self.gen_values = [val] * RANDOM_BATCH_SIZE
        
        self.refill_values()
        
        
    def refill_values(self):
        self.index = 0
    
    def __str__(self):
        return f'Single Value of {self.mean}'