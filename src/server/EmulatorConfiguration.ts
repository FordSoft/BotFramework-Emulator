export class EmulatorConfiguration {
    static readonly BotPortName: string = "--bot-port=";
    static readonly EmulatorPortName: string = "--emulator-port=";
    static readonly ProxyName: string = "--proxy=";

    private static instance: EmulatorConfiguration;

    public BotApplicationPort: number;
    public EmulatorApplicationPort: number;
    public Proxy: string;

    static getInstance() {
        if (!EmulatorConfiguration.instance) {
            EmulatorConfiguration.instance = new EmulatorConfiguration();
        }
        return EmulatorConfiguration.instance;
    }

    public ParseArguments(args: string[]) {
        args.forEach(arr => {
            if (arr.startsWith(EmulatorConfiguration.BotPortName)) {
                this.BotApplicationPort = parseInt(arr.slice(EmulatorConfiguration.BotPortName.length));
                console.log(`Parse configuration value for '${EmulatorConfiguration.BotPortName}' : '${this.BotApplicationPort}'`);
            }
            if (arr.startsWith(EmulatorConfiguration.EmulatorPortName)) {
                this.EmulatorApplicationPort = parseInt(arr.slice(EmulatorConfiguration.EmulatorPortName.length));
                console.log(`Parse configuration value for '${EmulatorConfiguration.EmulatorPortName}' : '${this.EmulatorApplicationPort}'`);
            }
            if (arr.startsWith(EmulatorConfiguration.ProxyName)) {
                this.Proxy = arr.slice(EmulatorConfiguration.ProxyName.length);
                console.log(`Parse configuration value for '${EmulatorConfiguration.ProxyName}' : '${this.Proxy}'`);
            }

        });
    }
}