include env.mk

prod_build:
	./scripts/deployment/build.sh $(USER_NAME) $(REPO_NAME)

prod_setup:
	./scripts/deployment/setup.sh "$(NAMESPACE)" "$(REPO_NAME)"

prod_teardown:
	kubectl -n $(NAMESPACE) delete -f infra/deployment.yaml --ignore-not-found

.PHONY: prod_build prod_setup prod_teardown
