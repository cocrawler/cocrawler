def expand_seeds(seeds):
    ret = []

    if seeds.get('Hosts', []):
        for h in seeds['Hosts']:
            ret.append(h)

    if seeds.get('Files', []):
        for name in seeds['Files']:
            with open(name, 'r') as f:
                for line in f:
                    if '#' in line:
                        line, _ = line.split('#', maxsplit=1)
                    if line.strip() == '':
                        continue
                    ret.append(line.rstrip())

    # sitemaps are a little tedious, so I'll implement later.
    # needs to be getched and then xml parsed and then <urlset ><url><loc></loc> elements extracted

    return ret
