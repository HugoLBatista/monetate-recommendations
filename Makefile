##############################################################################
# Goals for interacting with a integration box
#
# These goals sync files to, or execute commands on, a integration box.
##############################################################################
ifndef SSHUSER
SSHUSER := $(USER)
endif

usrhome := /home/$(SSHUSER)
blddir := $(usrhome)/monetate-recommendations/
venv := $(usrhome)/venv/monetate-recommendations
venv_bin := $(venv)/bin


create-environment:
	(cd $(blddir) && \
 	sudo ./scripts/install_virtualenv.sh $(venv) -d $(venv) --no-input)

intbox-sync:
	(host -tA $(INTEGRATIONBOX) > /dev/null && \
	rsync --delete -crlpv -e ssh --exclude-from .rsync-exclude.txt ./ $(SSHUSER)@$(INTEGRATIONBOX):$(blddir)) || \
	(echo "Cannot rsync source to INTEGRATIONBOX=\"$(INTEGRATIONBOX)\"" && false)

install: \
	install-recommendations

build-recommendations: create-environment
	cd $(blddir)
	sudo $(venv_bin)/pip install --upgrade 'setuptools<45.0.0'
	$(venv_bin)/python setup.py bdist_wheel

install-recommendations: build-recommendations
	cd $(blddir)
	sudo $(venv_bin)/pip uninstall monetate-recommendations -y
	sudo $(venv_bin)/pip install `ls -r dist/monetate_recommendations* | head -n 1`

test: \
	test-recommendations

test-recommendations: install-recommendations
	@echo "Testing monetate-recommendations"
	@echo "NOTE: Schema reset assumes webui/monetate working directory."
	(cd $(blddir) && \
	#PYTHONPATH=$(venv_bin) \ --- this breaks things. not sure why...
	sudo python manage.py test $(TEST) -v 2 --noinput)


# Alias targets for remote execution of make commands.
# No file target may start with intbox-.
intbox-%:
	ssh -t $(SSHUSER)@$(INTEGRATIONBOX) "TEST=$(TEST) make -C $(blddir) $*"