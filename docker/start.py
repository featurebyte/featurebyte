import json
import os
import subprocess


def which(program):
    import os

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def read_docker_creds():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    cred_path = script_dir + "/creds.json.b64"
    print("# Reading credentials from " + cred_path)
    docker_creds_file = open(cred_path)
    data = docker_creds_file.read()
    docker_creds_file.close()
    return data


def restore_docker_cfg():
    with open(os.path.expanduser("~/.docker/config.json.old"), "rb") as backup_file:
        with open(os.path.expanduser("~/.docker/config.json"), "wb") as file:
            file.write(backup_file.read())


def edit_docker_cfg():
    # Check if file exists
    if not os.path.isfile(os.path.expanduser("~/.docker/config.json")):
        with open(os.path.expanduser("~/.docker/config.json"), "w") as file:
            file.write("{}")
    else:
        with open(os.path.expanduser("~/.docker/config.json.old"), "wb") as backup_file:
            with open(os.path.expanduser("~/.docker/config.json"), "rb") as file:
                backup_file.write(file.read())

    # Reading
    with open(os.path.expanduser("~/.docker/config.json")) as docker_cfg_file:
        docker_cfg: dict = json.load(docker_cfg_file)

        # Config parsing
        #    This is required for windows as by default windows credential helper
        #    does not work for `docker login`
        if "auths" in docker_cfg:
            auths = docker_cfg["auths"]
        else:
            auths = {}

        # Removing conflicting credential helper
        if "credsStore" in docker_cfg:
            docker_cfg.pop("credsStore")
        if "credStore" in docker_cfg:
            docker_cfg.pop("credStore")

        if "us-central1-docker.pkg.dev" in auths:
            print("credentials have been supplied")
        else:
            auths["us-central1-docker.pkg.dev"] = {"auth": read_docker_creds()}
        docker_cfg["auths"] = auths

    # Writing
    with open(os.path.expanduser("~/.docker/config.json"), "w") as docker_cfg_file:
        json.dump(docker_cfg, docker_cfg_file, indent=4)


if __name__ == "__main__":
    # Check if docker exists
    if not which("docker"):
        print("You are missing docker.")
        print("Or docker is not present in your path.")
        print("Please install docker or configure your path variable")

    # Add Config value
    print("# Setting docker configuration")
    edit_docker_cfg()

    # Delete old deployment
    print("# Deleting old deployment")
    subprocess.run("docker compose down".split(" "))

    # Docker compose does not delete the container sometimes
    subprocess.run("docker container rm featurebyte-server featurebyte-docs".split(" "))

    # Pulling new image
    print("# Pulling new image")
    subprocess.run("docker compose pull".split(" "))

    # Reverting docker credentials
    print("# Setting docker configuration")
    restore_docker_cfg()

    # Starting it up
    print("# Starting server")
    subprocess.run("docker compose up".split(" "))
