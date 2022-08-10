def fn1a():
    print("first use")
    print("second use")


def fn1b(x, y):
    return x + y


def fn1c(z):
    print("third use", z)
    import pdb; pdb.set_trace()
