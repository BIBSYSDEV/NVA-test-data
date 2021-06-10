import import_authors
import import_users
import import_customers
import import_publications
import create_cognito_user

create_cognito_user.run()
import_customers.run()
import_users.run()
import_authors.run()
import_publications.run()
