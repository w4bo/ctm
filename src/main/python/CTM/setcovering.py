l = 1
weights = {
    frozenset(['a']): 1 * l,
    frozenset(['b']): 1 * l,
    frozenset(['c']): 1 * l,
    frozenset(['d']): 1 * l,
    frozenset(['a', 'b']): 1 * 2 * l,
    frozenset(['c', 'd']): 1 * 2 * l,
    frozenset(['b', 'c']): 0.2 * 2 * l,
    frozenset(['a', 'b', 'c', 'd']): 0.2 * 4 * l,

    frozenset(['e']): 1 * l,
    frozenset(['f']): 1 * l,
    frozenset(['g']): 1 * l,
    frozenset(['h']): 1 * l,
    frozenset(['e', 'f']): 0.5 * 2 * l,
    frozenset(['e', 'g']): 0.5 * 2 * l,
    frozenset(['f', 'h']): 0.5 * 2 * l,
    frozenset(['g', 'h']): 0.5 * 2 * l,
    frozenset(['e', 'f', 'g', 'h']): 0.1 * 4 * l,

    frozenset(['x']): 1 * l,
    frozenset(['y']): 1 * l,
    frozenset(['x', 'y']): 0.2 * 2 * l,
}


def set_cover(universe, subsets):
    """Find a family of subsets that covers the universal set"""
    covered = set()
    cover = []

    # Greedily add the subsets with the most uncovered points
    while covered != universe:
        print(covered)
        # subset = max(subsets, key=lambda s: len(s - covered))  # set cover
        subset = min([s for s in subsets if len(s - covered) > 0], key=lambda s: -weights[s])  # set cover
        print(subset)
        print(-weights[subset] / len(subset - covered))
        subsets.remove(subset)
        cover.append(subset)
        covered |= set([x for x in subset])
    return cover


subsets = set(weights.keys())
universe = set([item for sublist in subsets for item in list(sublist)])
cover = set_cover(universe, subsets)
print(cover)
