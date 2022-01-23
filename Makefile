test:
	python3 esmf_branch_summary.py ../esmf-test-artifacts/ cheyenne -b develop -l 'info'

cheyenne:
	python3 esmf_branch_summary.py ../esmf-test-artifacts/ cheyenne -b develop -l 'info'

acorn:
	python3 esmf_branch_summary.py ../esmf-test-artifacts/ acorn -b develop -l 'info'

chianti:
	python3 esmf_branch_summary.py ../esmf-test-artifacts/ chianti -b develop -l 'info'

jet:
	python3 esmf_branch_summary.py ../esmf-test-artifacts/ jet -b develop -l 'info'
