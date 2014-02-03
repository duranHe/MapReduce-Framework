package Slave;

import Configuration.*;

public class SlaveManager {
	private static Slave slave;
	public SlaveManager() {
	}

	public static void main(String[] args) throws Exception {
		Configuration.setup();
		slave = new Slave();
		slave.doSlave();
	}
}
