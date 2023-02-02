import subprocess

if __name__ == "__main__":
    print("# Stopping featurebyte-beta")
    p = subprocess.Popen("docker compose down".split(" "))
    p.wait()

    # Docker compose does not delete the container sometimes
    # It is okay if there is an error just making sure the containers are deleted
    p = subprocess.Popen("docker container rm featurebyte-server featurebyte-docs".split(" "))
    p.wait()
