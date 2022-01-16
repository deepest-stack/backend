import math
from functools import reduce


def nmi(c_a, c_b):
    assert sum(map(len, c_a)) == sum(map(len, c_b))
    n = sum(map(len, c_a))
    h_a = sum(map(lambda x: len(x)*math.log(len(x)/n), c_a))
    h_b = sum(map(lambda x: len(x)*math.log(len(x)/n), c_b))
    h_ab = 0
    for s1 in c_a:
        for s2 in c_b:
            c_ij = s1.intersection(s2).__len__()
            h_ab += c_ij * math.log(c_ij*n/len(s1)/len(s2)+10e-6)
    return -2 * h_ab / (h_a + h_b)


def ari(c_a, c_b):
    division_a = dict()
    for i, c in enumerate(c_a):
        for vid in c:
            division_a[vid] = i
    division_b = dict()
    for i, c in enumerate(c_b):
        for vid in c:
            division_b[vid] = i
    a_11, a_00, a_10, a_01 = 0, 0, 0, 0
    vertices = reduce(lambda s1, s2: s1.union(s2), c_a)
    for v1 in vertices:
        for v2 in vertices:
            if division_a[v1] == division_a[v2] and division_b[v1] == division_b[v2]:
                a_11 += 1
            elif division_a[v1] != division_a[v2] and division_b[v1] != division_b[v2]:
                a_00 += 1
            elif division_a[v1] == division_a[v2] and division_b[v1] != division_b[v2]:
                a_10 += 1
            elif division_a[v1] != division_a[v2] and division_b[v1] == division_b[v2]:
                a_01 += 1
            else:
                pass
    var1 = (a_11+a_01)*(a_11+a_10)/a_00
    var2 = (a_11+a_01+a_11+a_10)/2
    var3 = (a_11+a_01)*(a_11+a_10)/a_00
    return (a_11-var1)/(var2-var3)