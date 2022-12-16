  #		"This script will set up a new python virtual environment"
  #		"It is currently required that you run this script as root"
  #
  #		"options"
  #		"-d --dir	: the directory the virtual environment will be created in"
  #		"-u --user : the user the virtual environment is being created for"
  #		"-h --help	: displays help message"
  #   "--no-input: sets default inputs. for use in other scripts"
  #
  #		"example usage:"
  #		"./setup_virtualenv.sh -u root -d monetate-recommendations"

isNoInput="false"

for ((arg=1; arg<=$#; arg++));
do 
	val=$((arg+1))
	if [ "${!arg}" == "--help" ]; then
		echo "This script will set up a new python virtual environment"
		echo "It is currently required that you run this script as root"
		echo ""
		echo "[ options ]"
		echo "-d --dir	: the name of the virtual environment being created"
		echo "-h --help	: displays this help message, then exit"
		echo "--no-input: sets default inputs. for use in other scripts"
		echo ""
		echo "example usage:"
		echo "./setup_virtualenv.sh -u root -d monetate-recommendations"
		exit 0
	fi
	if [[ "${!arg}" == "-d" || "${!arg}" == "--dir" ]]; then
		virtualenv_dir="${!val}"
	fi
	if [ "${!arg}" == "--no-input" ]; then
    isNoInput="true"
	fi
done

if [ `whoami` != "root" ]; then
	echo "script must be run with sudo!"
	exit -1
fi
if [ -z "$virtualenv_dir" ]; then
	read -p 'Enter the file path of the virtual environment to be created: ' virtualenv_dir
fi

folder_exists()
{
    echo "existing virtual environment found at $1"
    echo "skipping environment creation."
    exit 0
}

virtualenv_bin=$virtualenv_dir/bin
input=""
if [ ! -d "$virtualenv_dir" ]; then
	mkdir $virtualenv_dir
	echo "$virtualenv_dir created!"
else
  if [ "$isNoInput" == "true" ]; then
    folder_exists $virtualenv_dir
  else
    echo "$virtualenv_dir exists!"
    read -p "would you like to delete and recreate it? (y/N) " x
    input=${x:-N}
    if [ "$x" == "y" ]; then
      rm -rf $virtualenv_dir
      mkdir $virtualenv_dir
      echo "$virtualenv_dir created!"
    else
      folder_exists $virtualenv_dir
    fi
  fi
fi
echo "getting virtualenv package"
pip install virtualenv==15.0.0
echo "creating virtual environment in $virtualenv_dir"
python -m virtualenv "$virtualenv_dir"
echo "virtual environment created!"
