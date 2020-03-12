##############################################################################
# Goals for interacting with a integration box
#
# These goals sync files to, or execute commands on, a integration box.
##############################################################################
ifndef SSHUSER
SSHUSER := $(USER)
endif

intbox-sync:
	(host -tA $(INTEGRATIONBOX) > /dev/null && \
	rsync --delete -crlpv -e ssh --exclude-from .rsync-exclude.txt ./ $(SSHUSER)@$(INTEGRATIONBOX):~/monetate-io/) || \
	(echo "Cannot rsync source to INTEGRATIONBOX=\"$(INTEGRATIONBOX)\"" && false)

