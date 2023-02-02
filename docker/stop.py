import subprocess

if __name__ == "__main__":
    print("# Stopping featurebyte-beta")
    subprocess.run("docker compose down".split(" "))

    # Docker compose does not delete the container sometimes
    # It is okay if there is an error just making sure the containers are deleted
    subprocess.run("docker container rm featurebyte-server featurebyte-docs".split(" "))
