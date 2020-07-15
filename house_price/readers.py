from csv import DictReader, DictWriter
import numpy as np
from house_price.filenames import file_names


def process_raw(row):
    sep = ":"
    output = {'zipcode': row['zipcode'].zfill(5),
              'state': row['state'],
              'city': row['city'],
              'metro': row['metro'],
              'county': row['county'],
              'state_metro': sep.join([row['state'], row['metro']]),
              'state_metro_county': sep.join([row['state'], row['metro'], row['county']]),
              'state_metro_county_city': sep.join([row['state'], row['metro'],
                                                   row['county'], row['city']]),
              'avg_price': float(row['avg_price'])}

    return output


def stream_data():
    stream = DictReader(open(file_names['zillow_zip'], 'r'))
    return (process_raw(row) for row in stream)


def get_price_by_zip_lookup():
    return {i['zipcode']: i['avg_price'] for i in stream_data()}


def get_pop_by_zip_lookup():
    stream = DictReader(open(file_names['pop_zip'], 'r', encoding='utf-8-sig'))

    return {i['zipcode'].zfill(5): float(i['population']) for i in stream}


def get_zip_sampler(seed=None):
    zip_lookup = get_pop_by_zip_lookup()
    items = zip_lookup.items()
    zips, pops = zip(*items)

    sum_pops = sum(pops)
    pop_cum = np.cumsum(pops)/sum_pops

    np.random.seed(seed)

    def sampler():
        rand = np.random.rand()
        index = np.where(pop_cum > rand)[0][0]
        return zips[index]

    return sampler


def get_samples(num):
    data = stream_data()
    look_up = {i['zipcode']: i for i in data}
    sampler = get_zip_sampler()
    output = []
    sigma = 10000.0
    sf_min = 800
    sf_sigma = 0.8
    yard_size_sigma = 1.5
    sf_amp = 1000
    sf_scaling = 0.9
    price_bed = 7000
    price_baths = 4000
    price_year = 100
    yard_min = 100
    yard_amp = 1800
    yard_price = 20.0
    yard_price_2 = 9.0
    kitchen_price = 20000

    pool_price_default = 4000.0
    # create some non-linearity
    pool_price = {'CA': 18000,
                  'TX': 12000,
                  'AZ': 10000,
                  'NM': 8000,
                  'LA': 9000,
                  'FL': 12000,
                  'AL': 8000,
                  'HI': 11000}

    park_price_default = 3000
    park_price = {'New York-Newark-Jersey City': 30000,
                  'Boston-Cambridge-Newton': 15000,
                  'Miami-Fort Lauderdale-West Palm Beach': 12000,
                  'Washington-Arlington-Alexandria': 10000,
                  'San Francisco-Oakland-Hayward': 20000}

    style_prices = {'ranch': -9000,
                    'colonial': 2000,
                    'victorian': 18000,
                    'tudor': 14000,
                    'craftsman': -1000,
                    'cottage': 1000,
                    'mediterranean': 3000,
                    'contemporary': 5000,
                    'other': -2000}

    styles = list(style_prices.keys())
    n_styles = len(styles)
    mean_style_price = np.mean(list(style_prices.values()))

    n = 0
    while True:
        zip = sampler()
        row = look_up.get(zip)
        if row is None:
            continue

        price = row['avg_price']

        square_feet = int(round(sf_min + sf_amp * np.exp(sf_sigma * np.random.randn())))
        square_feet_yard = int(round(yard_min + yard_amp * np.exp(yard_size_sigma * np.random.randn())))

        if row['metro'] == 'New York-Newark-Jersey City' and np.random.rand() < 0.95:
            square_feet_yard = 0

        lot_size = square_feet + square_feet_yard
        row['lot_size_sf'] = lot_size

        beds = np.random.randint(1, 8)
        baths = np.random.randint(1, 5)
        year_built = int(round(1780 + 245 * np.random.rand()))
        year_built = min(year_built, 2020)
        row['beds'] = beds
        row['baths'] = baths
        row['year_built'] = year_built

        kitchen_refurbished = False
        if year_built < 1990:
            kitchen_refurbished = np.random.rand() < 0.2
            if year_built < 1970:
                kitchen_refurbished = np.random.rand() < 0.6

        kitchen_refurbished = 1 * kitchen_refurbished

        row['kitchen_refurbished'] = kitchen_refurbished
        row['square_feet'] = square_feet
        price += price_bed * (beds-4)
        price *= (square_feet/2160)**sf_scaling
        price += price_baths * (baths - 2.5)
        price += abs(year_built-1900) * price_year
        price += kitchen_price * kitchen_refurbished

        if square_feet_yard < 3000:
            price += yard_price * square_feet_yard
        else:
            price += yard_price * 3000 + yard_price_2 * (square_feet_yard-3000)
        price -= 10000

        noise = np.random.randn() * sigma
        price += noise

        if price <= 0:
            continue

        # pool
        pool_add = -2000

        if row['state'] in pool_price:
            pool = np.random.rand() < 0.4
            pool_add += pool * pool_price[row['state']]
        else:
            pool = np.random.rand() < 0.1
            pool_add += pool * pool_price_default

        row['pool'] = 1 * pool
        price += pool_add

        # parking
        park_add = -1000

        if row['metro'] in park_price:
            park = 1 * (np.random.rand() < 0.15)
            park_add += park * park_price[row['metro']]
        else:
            park = 1 * (np.random.rand() < 0.75)
            park_add += park * park_price_default

        row['parking'] = park

        price += park_add

        style = np.random.choice(styles)
        style_add = style_prices[style] - mean_style_price

        price += style_add
        row['style'] = style

        if np.random.rand() < 0.1:
            multi_family = 1
        else:
            multi_family = 0

        row['multi_family'] = multi_family

        if multi_family:
            price *= 0.7
        else:
            price *= 1.05

        # done price
        price = int(price)
        row['price'] = price

        output.append(row)
        n += 1
        if n == num:
            return output


def process_input(row):
    out_row = row.copy()
    del_keys = ['state_metro',
                'state_metro_county',
                'state_metro_county_city',
                'avg_price',
                'state',
                'metro',
                'county',
                'city']

    for dk in del_keys:
        del out_row[dk]

    return out_row


def get_data(num):
    samples = get_samples(num)
    return [process_input(i) for i in samples]


def write_data(num):
    data = get_data(num)
    fields = list(data[0].keys())
    filename = file_names['house_data'].format(num=num)
    dr = DictWriter(open(filename, 'w'), fieldnames=fields)
    dr.writeheader()
    dr.writerows(data)


