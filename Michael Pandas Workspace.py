# Pandas Workspace

# %%
import pandas as pd
import time

# Create an object that is a link to data
birds_link = 'https://portal.edirepository.org/nis/dataviewer?packageid=knb-lter-cap.256.10&entityid=53edaa7a0e083013d9bf20322db1780e'

# Create the same data frame using pandas
birds_pandas = pd.read_csv(birds_link)

# Compare your pandas data frame with your dask data frame
print(type(birds_pandas))

# %%
start_time = time.time()

birds_pandasx2 = pd.concat([birds_pandas, birds_pandas])

birds_pandasx4 = pd.concat([birds_pandasx2, birds_pandasx2])

birds_pandasx8 = pd.concat([birds_pandasx4, birds_pandasx4])

birds_pandasx16 = pd.concat([birds_pandasx8, birds_pandasx8])

birds_pandasx32 = pd.concat([birds_pandasx16, birds_pandasx16])

birds_pandasx64 = pd.concat([birds_pandasx32, birds_pandasx32])

birds_pandasx128 = pd.concat([birds_pandasx64, birds_pandasx64])

birds_pandasx256 = pd.concat([birds_pandasx128, birds_pandasx128])

birds_pandasx512 = pd.concat([birds_pandasx256, birds_pandasx256])

birds_pandasx1024 = pd.concat([birds_pandasx512, birds_pandasx512])

birds_pandasx2048 = pd.concat([birds_pandasx1024, birds_pandasx1024])

birds_pandasx4096 = pd.concat([birds_pandasx2048, birds_pandasx2048])

end_time = time.time()

print(f"It took {end_time-start_time} seconds")

print(birds_pandasx4096.head)


# %%

birds_pandasx4096['distancex100'] = birds_pandasx4096['distance']*100

# %%