.PHONY: check-syntax

all: chicago chicago/client chicago/server

chicago:
	$(MAKE) -C src/main/java/com/xjeffrose/chicago

chicago/client:
	$(MAKE) -C src/main/java/com/xjeffrose/chicago/client

chicago/server:
	$(MAKE) -C src/main/java/com/xjeffrose/chicago/server

test:
	java -cp lib/*.jar org.junit.runner.JUnitCore src/test/com/xjeffrose/chicago/*.java

jar:
	jar cvf test.jar target/test/mod1/*.class target/test/mod2/*.class target/chica/*.class
	jar cfe test.jar test.Main.main target/test/Main.class
