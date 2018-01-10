
library(dplyr)
library(sp)
library(rgdal)
library(rgeos)
library(reshape)

# read the TAPS data into a spatial points data frame
filename_taps       <- 'M:/Data/OnBoard/Data and Reports/_geocoding Standardized/TAPs/taps_lat_long.csv'
df_taps             <- read.csv(filename_taps) %>% select(N, MODE, LAT, LONG) 
df_taps$MODE        <- as.factor(recode(df_taps$MODE, `1` = "local bus", `2` = "express bus", `3` = "ferry", `4` = "light rail", `5` = "heavy rail", `6` = "commuter rail"))
spdf_taps           <- SpatialPointsDataFrame(df_taps[,c("LONG","LAT")], df_taps[,c("N","MODE")])

# read the boarding and alighting locations
filename_obs_board  <- 'M:/Data/OnBoard/Data and Reports/_geocoding Standardized/boarding_places_to_be_geocoded.csv'
filename_obs_alight <- 'M:/Data/OnBoard/Data and Reports/_geocoding Standardized/alighting_places_to_be_geocoded.csv'
filename_out        <- 'M:/Data/OnBoard/Data and Reports/_geocoding Standardized/boarding_alighting_places_geocoded_R.csv'

df_obs_board        <- read.csv(filename_obs_board, stringsAsFactors=FALSE)
df_obs_alight       <- read.csv(filename_obs_alight, stringsAsFactors=FALSE)
spdf_obs_board      <- SpatialPointsDataFrame(df_obs_board [,c("first_board_lon","first_board_lat")], df_obs_board [,c("Unique_ID","first_board_tech")])
spdf_obs_alight     <- SpatialPointsDataFrame(df_obs_alight[,c("last_alight_lon","last_alight_lat")], df_obs_alight[,c("Unique_ID","last_alight_tech")])

print("Boarding Tech")
print(table(df_obs_board$first_board_tech))

print("Alight Tech")
print(table(df_obs_alight$last_alight_tech))

print("Taps MODE")
print(table(df_taps$MODE))
# df_obs_board['tap'] = -999
# df_obs_board['tap_dist'] = -999.99999

closest_board_df_all <- data.frame()
closest_alight_df_all <- data.frame()

# split by tech
for (mode in levels(df_taps$MODE)) {
  print(mode)
  # select the taps, boards or alights for the mode
  spdf_taps_mode       <- spdf_taps      [ which(spdf_taps$MODE==mode),]
  
  for (board_alight in c("boards","alights")) {
    if (board_alight=="boards") {
      spdf_obs_mode    <- spdf_obs_board [ which(spdf_obs_board$first_board_tech==mode ),]
    } else {
      spdf_obs_mode    <- spdf_obs_alight[ which(spdf_obs_alight$last_alight_tech==mode),]
    }

    print(paste("  Matching",nrow(spdf_obs_mode),board_alight,"with",nrow(spdf_taps_mode),"taps"))

    # this produces rows (board locs) x columns (taps)
    distances <- gDistance(spdf_obs_mode, spdf_taps_mode, byid=TRUE)
    # print(dim(distances))

    min_dists <- data.frame(apply(distances, 2, min))
    colnames(min_dists) <- "tap_dist"
      
    min.d <- apply(distances, 2, which.min)
    # min.d is an array of ints, from 1-nrow(spdf_taps_mode)
    closest_taps_mode <- data.frame(spdf_taps_mode[min.d,])

    closest_df <- cbind(data.frame(spdf_obs_mode$Unique_ID), min_dists, data.frame(spdf_taps_mode[min.d,]))
    # column names are: spdf_obs_mode.Unique_ID, tap_dist, N, MODE, LONG, LAT, optional
    # rename for clarity
    closest_df <- select(closest_df, Unique_ID=spdf_obs_mode.Unique_ID, tap_dist, N, MODE, tap_LONG=LONG, tap_LAT=LAT)

    # save the mapping
    if (board_alight=="boards") {
      closest_board_df_all <- rbind(closest_board_df_all, closest_df)
    } else {
      closest_alight_df_all <- rbind(closest_alight_df_all, closest_df)
    }
  }
}


# join original board info + board tap
board_df  <- left_join( select(df_obs_board, Unique_ID, first_board_lat, first_board_lon),
                        select(closest_board_df_all, Unique_ID, board_tap=N, board_tap_dist=tap_dist, board_tap_lon=tap_LONG, board_tap_lat=tap_LAT) )
# join original alight info + alight tap
alight_df <- left_join( select(df_obs_alight, Unique_ID, last_alight_lat, last_alight_lon),
                        select(closest_alight_df_all, Unique_ID, alight_tap=N, alight_tap_dist=tap_dist, alight_tap_lon=tap_LONG, alight_tap_lat=tap_LAT) )

full_out <- full_join(board_df, alight_df)
# sort by Unique_ID
full_out <- full_out[order(full_out$Unique_ID),] 

# keep original columns: Unique_ID,first_board_lat,first_board_lon,board_tap,board_tap_dist,last_alight_lat,last_alight_lon,alight_tap,alight_tap_dist
write.table(select(full_out, Unique_ID,first_board_lat,first_board_lon,board_tap,board_tap_dist,last_alight_lat,last_alight_lon,alight_tap,alight_tap_dist), 
            filename_out, quote=FALSE, row.names=FALSE, sep=",", na="")
