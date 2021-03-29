const strftime = require('strftime');

const strftimePlugin = (option, dayjsClass, dayjsFactory) => {
    dayjsClass.prototype.strftime = function(args, local) {
        let modifiedFormatter;
        if (local) {
            modifiedFormatter = strftime.localizeByIdentifier(local)
        } else {
            modifiedFormatter = strftime
        };

		modifiedFormatter = modifiedFormatter.timezone(this.format('ZZ'));

        return modifiedFormatter(args, this.toDate())
    }
  }

module.exports = strftimePlugin;
