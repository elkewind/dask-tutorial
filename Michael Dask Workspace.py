# Dask Workspace

# %%
import pandas as pd
import dask.dataframe as dd

# Create an object that is a link to data
birds_link = 'https://portal.edirepository.org/nis/dataviewer?packageid=knb-lter-cap.256.10&entityid=53edaa7a0e083013d9bf20322db1780e'

# Create the same data frame using pandas
birds_pandas = pd.read_csv(birds_link)

# Compare your pandas data frame with your dask data frame
print(type(birds_pandas))

ddf = dd.from_pandas(birds_pandas, npartitions=5)
print(type(ddf))

#print(birds_pandas.head)




# %%

birds_pandasx2 = dd.multi.concat([birds_pandas, birds_pandas])

birds_pandasx4 = dd.multi.concat([birds_pandasx2, birds_pandasx2])

birds_pandasx8 = dd.multi.concat([birds_pandasx4, birds_pandasx4])

birds_pandasx16 = dd.multi.concat([birds_pandasx8, birds_pandasx8])

birds_pandasx32 = dd.multi.concat([birds_pandasx16, birds_pandasx16])

birds_pandasx64 = dd.multi.concat([birds_pandasx32, birds_pandasx32])

birds_pandasx128 = dd.multi.concat([birds_pandasx64, birds_pandasx64])

birds_pandasx256 = dd.multi.concat([birds_pandasx128, birds_pandasx128])

birds_pandasx512 = dd.multi.concat([birds_pandasx256, birds_pandasx256])

birds_pandasx1024 = dd.multi.concat([birds_pandasx512, birds_pandasx512])

birds_pandasx2048 = dd.multi.concat([birds_pandasx1024, birds_pandasx1024])

#birds_pandasx4096 = dd.multi.concat([birds_pandasx2048, birds_pandasx2048])

print(birds_pandasx2048.head)

# %%

birds_pandasx2048['distancex100'] = birds_pandasx2048['distance']*100

# %%

#birds_daskx2048 = dd.from_pandas(birds_pandasx2048, npartitions=2048)

birds_pandasx2048['distancex100'] = birds_pandasx2048['distance']*100

# %%