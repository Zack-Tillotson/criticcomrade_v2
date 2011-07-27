java -jar rt-etl.jar --move-run 2>&1
sleep 300
java -jar rt-etl.jar --from-current-lists 2>&1 1>> log/lists 
java -jar rt-etl.jar --from-website --threads 1 --max-runtime 1200 2>&1 1>> log/website & 
java -jar rt-etl.jar --from-queue --threads 5 --max-runtime 1200 2>&1 1>>log/queue 
