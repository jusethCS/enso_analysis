library(dplyr)
library(readxl)
library(sf)

geoglows_data = read_excel("out_years.xlsx", sheet = 1)
comids_basin = read_excel("out_years.xlsx", sheet = 2)
data = left_join(geoglows_data, comids_basin)
basins = st_read("nwsaffgs_ecuador_basins.shp")
basins = left_join(basins, data, by = "BASIN")
st_write(basins, "analysis_years.shp")



geoglows_data = read_excel("salida.xlsx", sheet = 1)
comids_basin = read_excel("salida.xlsx", sheet = 2)
data = left_join(geoglows_data, comids_basin)
basins = st_read("nwsaffgs_ecuador_basins.shp")
basins = left_join(basins, data, by = "BASIN")
st_write(basins, "salida_geog.shp")
plot(basins %>% select(events_82_83))


















#####################################################
# Cargar la biblioteca necesaria para trabajar con imágenes TIFF
library(lubridate)
library(dplyr)
library(raster)

# Directorio donde se encuentran los archivos TIFF organizados por día
get_values = function(start, end){
  setwd("D:/Informacion_Hidrometeorologica/precipitacion_satelital/MSWEP/datos")
  start = as.POSIXct(start)
  end = as.POSIXct(end) - 86400
  print(c(start, end))
  fechas = seq(start, end, by = "days") %>% format("%Y-%m-%d.tif")
  raster_stack = sum(stack(fechas))
  setwd("C:/Users/Lenovo/Desktop/SERVIR_Amazonia/TETHYS_APPS_GITHUB/tethys_apps_ecuador/enso_analysis/pacum")
  writeRaster(raster_stack, format(start, "%Y.tif"), overwrite=TRUE)
}

seq_fechas = seq(as.POSIXct("1980-01-01"), as.POSIXct("2022-01-01"), by = "1 year")

for(i in 1:length(seq_fechas) ){
  get_values(seq_fechas[i], seq_fechas[i+1])
}



setwd("C:/Users/Lenovo/Desktop/SERVIR_Amazonia/TETHYS_APPS_GITHUB/tethys_apps_ecuador/enso_analysis/pacum")
f = list.files(pattern = ".tif")
pnormal = mean(stack(f))


p1982 = mask(raster("1982_01.tif") / pnormal, basins)
p1983 = mask(raster("1983_01.tif") / pnormal, basins)
p1997 = mask(raster("1997_01.tif") / pnormal, basins)
p1998 = mask(raster("1998_01.tif") / pnormal, basins)


writeRaster(p1982, "anomalities_1982.tif", overwrite=TRUE)
writeRaster(p1983, "anomalities_1983.tif", overwrite=TRUE)
writeRaster(p1997, "anomalities_1997.tif", overwrite=TRUE)
writeRaster(p1998, "anomalities_1998.tif", overwrite=TRUE)

