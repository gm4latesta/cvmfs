#Get operating system 
get_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
    elif type lsb_release >/dev/null 2>&1; then
        OS=$(lsb_release -si | tr '[:upper:]' '[:lower:]')
    elif [ -f /etc/lsb-release ]; then
        . /etc/lsb-release
        OS=$(echo $DISTRIB_ID | tr '[:upper:]' '[:lower:]')
    elif [ -f /etc/debian_version ]; then
        OS="debian"
    else
        OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    fi
    echo $OS
}

#Verify operating system
OS=$(get_os)
echo "Operating system: $OS"

if [[ "$OS" == "centos" ]]; then
    packages=("tcsh" "gcc" "gcc-gfortran" "gcc-c++")
elif [[ "$OS" == "ubuntu" ]]; then
    packages=("tcsh" "gcc" "gfortran" "g++")
else
    echo "Operating system not supported"
    exit 1
fi

#Install needed packages according to the operating system
for package in "${packages[@]}"; do
    if dpkg-query -W -f='${Status}\n' $package 2>/dev/null | grep -q "install"; then
        echo "$package not installed. Starting installation..."
#        if [[ "$OS" == "centos" ]]; then
#            sudo yum install -y $package
#        elif [[ "$OS" == "ubuntu" ]]; then
#            sudo apt-get install -y $package
#        fi
    else
        echo "$package already installed. Nothing to do"
    fi
done
