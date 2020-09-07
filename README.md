# CollocationExtraction

Map-Reduce program which produces a list of top-100 collocations from the Google 2-grams dataset for each decade (1990-1999, 2000-2009, etc.) for English and Hebrew,
with their log likelihood ratios (in descending order).

Collocation is a sequence of words that co-occur more often than would be expected by chance.

In this program, we will use log likelihood ratio in order to determine whether a given pair of ordered words is a collocation.
