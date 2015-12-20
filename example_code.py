test = {
        510:'Drug Manufacturers - Major',
        511:'Drug Manufacturers - Other'
    }
dataset = sector(test,'2009-01-01','2009-12-31')
results1 = dataset.get_gainers('2009-10-08',5)
results2 = dataset.get_losers('2009-10-08',5)
print(str(results1))
print(str(results2))
