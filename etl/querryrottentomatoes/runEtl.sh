# Firt update the current lists, then do the active movies, then get historical movies. We should change 
# this after we get all the movies at least once so that more time is spent making sure we get all the 
# latest movies
java -jar rt-etl.jar --move-run --from-current-lists 2>&1 1> logs/etl
java -jar rt-etl.jar --from-queue --threads 3 --max-runtime 120 2>&1 1> logs/etl
java -jar rt-etl.jar --from-queue --threads 3 --max-runtime 360 --no-update 2>&1 1> logs/etl
