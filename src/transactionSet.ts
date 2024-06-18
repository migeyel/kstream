import { ApiTransaction } from "./util";

/** An abstract set of transactions. */
export class TransactionSet {
    /** Contains mining reward transactions. */
    public readonly includeMined: boolean;

    /** Narrows down to transactions to/from some address. */
    public readonly address?: string;

    public constructor(includeMined: boolean, address?: string) {
        this.includeMined = includeMined;
        this.address = address;
    }

    public static all(): TransactionSet {
        return new TransactionSet(true);
    }

    public isSubset(other: TransactionSet): boolean {
        if (other.address && this.address != other.address) { return false; }
        if (this.includeMined && !other.includeMined) { return false; }
        return true;
    }

    public contains(transaction: ApiTransaction): boolean {
        if (!this.includeMined && transaction.type == "mined") { return false; }
        if (
            this.address &&
            transaction.from != this.address &&
            transaction.to != this.address
        ) { return false; }
        return true;
    }

    public getQuery(offset: number, limit: number, ascending: boolean): string {
        return string.format(
            "/lookup/transactions/%s?&order=%s&%s&offset=%d&limit=%d",
            this.address || "",
            ascending ? "ASC" : "DESC",
            this.includeMined ? "includeMined" : "",
            offset,
            limit,
        );
    }
}
