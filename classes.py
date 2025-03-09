class Backtest:
    def __init__(self, stock_a, stock_b, threshold, startingpos=1, def_shift=0.1):
        self.prices_a = stock_a
        self.prices_b = stock_b

        self.owned_a = startingpos
        self.owned_b = (1 - startingpos) * stock_a / stock_b
        self.owned_hist = [(self.owned_a, self.owned_b)]
        self.startval = stock_a[0]
        self.threshold = threshold
        self.default_shift = def_shift
        self.i = 0
        self.dur = 0
        self.min_duraton


    def shift_weights(self, diff):
        diff = min(self.owned_a, diff)

        self.owned_a -= diff
        cash = self.prices_a[self.i] * diff
        self.owned_b = cash / self.prices_b[self.i]


    def liquidate_b(self):
        if self.owned_b == 0:
            return
        else:
            cash = self.prices_b[self.i] * (self.owned_b)
            self.owned_b = 0
            self.owned_a += cash / self.prices_a[self.i]


    def step(self):
        i = self.i

        if self.prices_b[i] < self.threshold[0]:
            print(f'price falls below buy threshold: {self.prices_b[i]}')
            self.dur += 1
            self.shift_weights(self.default_shift)
        elif self.prices_b[i] > self.threshold[1]:
            print(f'price above sell threshold: {self.prices_b[i]}')
            self.dur = 0
            self.liquidate_b()

        self.i += 1
        self.owned_hist.append((self.owned_a, self.owned_b))

        print(self)


    def nstep(self, n):
        for _ in range(n):
            self.step()


    def get_portfolio_value(self, ret=True):
        pos_a = self.owned_a * self.prices_a[self.i]
        pos_b = self.owned_b * self.prices_b[self.i]
        
        val = pos_a + pos_b
        if ret:
            return val / self.startval
        else:
            return val


    def __str__(self):
        output = (
            f'SP price: {self.prices_a[self.i]}\n'
            f'UVIX: {self.prices_b[self.i]}\n'
            f'Porfolio: {self.owned_a} in SP & {self.owned_b} in UVIX\n'
            f'Total Value: {self.get_portfolio_value()}'
        )

        return output




