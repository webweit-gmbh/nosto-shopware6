<?php

declare(strict_types=1);

namespace Nosto\NostoIntegration\Migration;

use Doctrine\DBAL\Connection;
use Shopware\Core\Framework\Migration\MigrationStep;
use Shopware\Core\Framework\Uuid\Uuid;

/**
 * @internal
 */
class Migration1760541154AddChangelogEntityIdIndex extends MigrationStep
{
    public function getCreationTimestamp(): int
    {
        return 1760541154;
    }

    public function update(Connection $connection): void
    {
        $connection->executeStatement('create index nosto_integration_entity_id_idx
    on nosto_integration_entity_changelog (entity_id);');
    }

    public function updateDestructive(Connection $connection): void
    {
        // implement update destructive
    }
}
