# Dask Workspace

# %%
from timeit import timeit
import dask.dataframe as dd
import time

# Create an object that is a link to data
birds_link = 'https://portal.edirepository.org/nis/dataviewer?packageid=knb-lter-cap.256.10&entityid=53edaa7a0e083013d9bf20322db1780e'

# Create the same data frame using pandas
birds_dask = dd.read_csv(birds_link)

# Compare your pandas data frame with your dask data frame
print(type(birds_dask))


# %%

#%%timeit
start_time = time.time()

birds_daskx2 = dd.multi.concat([birds_dask, birds_dask])

birds_daskx4 = dd.multi.concat([birds_daskx2, birds_daskx2])

birds_daskx8 = dd.multi.concat([birds_daskx4, birds_daskx4])

birds_daskx16 = dd.multi.concat([birds_daskx8, birds_daskx8])

birds_daskx32 = dd.multi.concat([birds_daskx16, birds_daskx16])

birds_daskx64 = dd.multi.concat([birds_daskx32, birds_daskx32])

birds_daskx128 = dd.multi.concat([birds_daskx64, birds_daskx64])

birds_daskx256 = dd.multi.concat([birds_daskx128, birds_daskx128])

birds_daskx512 = dd.multi.concat([birds_daskx256, birds_daskx256])

birds_daskx1024 = dd.multi.concat([birds_daskx512, birds_daskx512])

birds_daskx2048 = dd.multi.concat([birds_daskx1024, birds_daskx1024])

birds_daskx4096 = dd.multi.concat([birds_daskx2048, birds_daskx2048])

birds_daskx4096['distancex100'] = birds_daskx4096['distance']*100

end_time = time.time()

print(f"It took {end_time-start_time} seconds")

# %%
