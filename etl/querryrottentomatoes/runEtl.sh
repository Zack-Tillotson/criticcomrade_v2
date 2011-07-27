java -jar rt-etl.jar --from-website --threads 4 --max-runtime 720 --from-queue --threads 3 --max-runtime 720 --move-run --from-current-lists 2>&1 1> logs/etl
