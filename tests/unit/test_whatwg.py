import cocrawler.whatwg

tests = (('utf8', 'utf-8'),
         ('866', 'ibm866'),
         ('latin2', 'iso-8859-2'),
         ('iso-8859-1', 'windows-1252'),  # yes
         ('cp1252', 'windows-1252'))


def test_encoding_map():
    for t, a in tests:
        assert cocrawler.whatwg.encoding_map(t) == a
