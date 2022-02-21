class ColumnMatcherEval:
    def __init__(self, all_tables, tables, res):
        self.tables = tables
        self.all_tables = all_tables
        self.res = res
        self._tp = None
        self._fp = None
        self._fn = None
        self._tn = None
        self.__all = None

    @property
    def tp(self):
        if self._tp is None:
            cnt = 0
            for r in self.res['items']:
                src_tbl = r['src_table'].split('_', 1)[1]
                target_tbl = r['target_table'].split('_', 1)[1]
                if src_tbl == target_tbl:
                    if self.tables[src_tbl]['rows']['src'] == r['src_row'] and \
                            self.tables[src_tbl]['rows']['target'] == r['target_row']:
                        cnt += 1
                        if (not self.res['bidi']) and (r['src_table'] != self.tables[src_tbl]['src']['name'] or
                        r['target_table'] != self.tables[src_tbl]['target']['name']):
                            cnt -= 1
                    elif self.res['bidi'] and self.tables[src_tbl]['rows']['src'] == r['target_row'] and \
                             self.tables[src_tbl]['rows']['target'] == r['src_row']:
                        cnt += 1
                        # print(f"{src_tbl}: {r['src_row']} -> {r['target_row']}")
            self._tp = cnt
        return self._tp

    @property
    def fp(self):
        if self._fp is None:
            self._fp = len(self.res['items']) - self.tp
        return self._fp

    @property
    def fn(self):
        if self._fn is None:
            # len(self.tables) --> All positive cases
            # self.tp --> predicated positive cases
            pos = 2 * len(self.tables) if self.res['bidi'] else len(self.tables)
            self._fn = pos - self.tp
        return self._fn

    @property
    def tn(self):
        if self._tn is None:
            self._tn = self._all - (self.tp + self.fp + self.fn)
        return self._tn

    @property
    def _all(self):
        if self.__all is None:
            cnt = 0
            for src in self.all_tables:
                for tgt in self.all_tables:
                    if tgt['name'] != src['name']:
                        cnt += ( len(src['titles']) * len(tgt['titles']) )
            self.__all = cnt
        return self.__all

    @property
    def accuracy(self):
        return (self.tp + self.tn) / self._all

    @property
    def precision(self):
        return self.tp/(self.tp + self.fp)

    @property
    def recall(self):
        return self.tp / (self.tp + self.fn)

    @property
    def f1(self):
        return (2 * self.precision * self.recall) / (self.precision + self.recall)

    def __str__(self):
        return f"TP:{self.tp}, FP={self.fp}, FN={self.fn}, TN={self.tn}, All_Col={self._all}, Acc={self.accuracy}, P={self.precision}, R={self.recall}, F1={self.f1}"

