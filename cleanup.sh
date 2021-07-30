if [[ -z "${CLEANUP}"] ]; then
  echo "Cleaning up previous run: GHA workspace dir."
  rm -rfv "${WORKSPACE}"
  echo "Cleaning up previous run: Docker local images."
  docker rmi -f $(docker images -a -q)
fi
