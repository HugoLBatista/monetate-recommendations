make intbox-sync
ssh $INTEGRATIONBOX "bash ./monetate-recommendations/scripts/install_latest.sh"
ssh $INTEGRATIONBOX "sudo bash ./monetate-recommendations/scripts/run_tests.sh"

