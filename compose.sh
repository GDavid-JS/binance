install_package() {
    package_name="$1"
    requirements="$PWD/requirements.txt"

    if [ -f "$requirements" ]; then

        if [[ "$package_name" == *==* ]]; then
            echo "$package_name" >> "$requirements"
        else
            package_version=$(pip show "$package_name" | grep Version | awk '{print $2}')
            echo "${package_name}==${package_version}" >> "$requirements"
        fi

    else
        echo "Файл $requirements не существует"
    fi
}

if [ "$1" = "run" ]; then
    docker compose up
elif [ "$1" = "rebuild" ]; then
    docker compose up --build
elif [ "$1" = "install" ]; then
    install_package "$2"
elif [ "$1" = "pip" ] && [ "$2" = "install" ]; then
    install_package "$3"
else
    echo "Invalid argument. Please specify 'install' or 'run'."
fi
