from dcicutils.misc_utils import ignored


def fn2a(z):
    print("third use", z)
    # This use won't count because it's marked...
    import pdb; pdb.set_trace()  # untallied use because NoQA is in comment


def fn2b(a, b, c):
    ignored(a, b, c)
    import pdb
    pdb.set_trace()  # Second tallied use

def fn3b():
    import pdb
    pdb.set_trace()  # Third tallied use
