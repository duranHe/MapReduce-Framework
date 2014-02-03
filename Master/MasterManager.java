package Master;

import Configuration.*;

public class MasterManager {
	private static Master master;
	public MasterManager() {
	}

	public static void main(String[] args) throws Exception {
		Configuration.setup();
		master = new Master();
		master.doMaster();
	}
}
