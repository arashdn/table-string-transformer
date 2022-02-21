from Transformation.Pattern import Pattern
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
import sys

def join(source, target, transformations, is_swapped=False, min_support=0.0, max_support=sys.float_info.max, verbose=False):

    if is_swapped:
        source, target = target, source

    source = set(source)  # not needed, we only search in target
    target = set(target)

    if verbose:
        print(f"{len(source)} in source and {len(target)} in target...")

    joins = set()
    cnt_tr = 1
    for trans in transformations:
        if len(trans.blocks) == 1 and type(trans.blocks[0]) == LiteralPatternBlock:  # Skip single literal
            if verbose:
                print(str(trans) + " skipped")
                cnt_tr += 1
            continue

        if verbose:
            print(f"Trans {cnt_tr}/{len(transformations)}: {trans}, ",end='')
            cnt_tr += 1

        cover = set()
        for src in source:
            out = trans.apply(src)
            if out in target:
                cover.add((src, out))

        support = len(cover)/len(source)
        added = min_support <= support <= max_support

        if verbose:
            st = " - added" if added else ""
            print(f"    {len(cover)}/{len(source)} = {support}{st}")

        if added:
            joins = joins.union(cover)

    return joins
